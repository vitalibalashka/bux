package bux

import (
	"context"
	"fmt"
	"time"

	"github.com/BuxOrg/bux/chainstate"
	"github.com/BuxOrg/bux/cluster"
	"github.com/BuxOrg/bux/notifications"
	"github.com/BuxOrg/bux/taskmanager"
	"github.com/bitcoin-sv/go-paymail"
	"github.com/bitcoin-sv/go-paymail/server"
	"github.com/mrz1836/go-cachestore"
	"github.com/mrz1836/go-datastore"
	zLogger "github.com/mrz1836/go-logger"
	"github.com/newrelic/go-agent/v3/newrelic"
)

type (

	// Client is the bux client & options
	Client struct {
		options *clientOptions
	}

	// clientOptions holds all the configuration for the client
	clientOptions struct {
		cacheStore            *cacheStoreOptions          // Configuration options for Cachestore (ristretto, redis, etc.)
		cluster               *clusterOptions             // Configuration options for the cluster coordinator
		chainstate            *chainstateOptions          // Configuration options for Chainstate (broadcast, sync, etc.)
		dataStore             *dataStoreOptions           // Configuration options for the DataStore (MySQL, etc.)
		debug                 bool                        // If the client is in debug mode
		encryptionKey         string                      // Encryption key for encrypting sensitive information (IE: paymail xPub) (hex encoded key)
		httpClient            HTTPInterface               // HTTP interface to use
		importBlockHeadersURL string                      // The URL of the block headers zip file to import old block headers on startup. if block 0 is found in the DB, block headers will mpt be downloaded
		itc                   bool                        // (Incoming Transactions Check) True will check incoming transactions via Miners (real-world)
		iuc                   bool                        // (Input UTXO Check) True will check input utxos when saving transactions
		logger                zLogger.GormLoggerInterface // Internal logging
		models                *modelOptions               // Configuration options for the loaded models
		newRelic              *newRelicOptions            // Configuration options for NewRelic
		notifications         *notificationsOptions       // Configuration options for Notifications
		paymail               *paymailOptions             // Paymail options & client
		taskManager           *taskManagerOptions         // Configuration options for the TaskManager (TaskQ, etc.)
		userAgent             string                      // User agent for all outgoing requests
	}

	// chainstateOptions holds the chainstate configuration and client
	chainstateOptions struct {
		chainstate.ClientInterface                        // Client for Chainstate
		options                    []chainstate.ClientOps // List of options
		broadcasting               bool                   // Default value for all transactions
		broadcastInstant           bool                   // Default value for all transactions
		paymailP2P                 bool                   // Default value for all transactions
		syncOnChain                bool                   // Default value for all transactions
	}

	// cacheStoreOptions holds the cache configuration and client
	cacheStoreOptions struct {
		cachestore.ClientInterface                        // Client for Cachestore
		options                    []cachestore.ClientOps // List of options
	}

	// clusterOptions holds the cluster configuration for Bux clusters
	// at the moment we only support redis as the cluster coordinator
	clusterOptions struct {
		cluster.ClientInterface
		options []cluster.ClientOps // List of options
	}

	// dataStoreOptions holds the data storage configuration and client
	dataStoreOptions struct {
		datastore.ClientInterface                       // Client for Datastore
		migrationDisabled         bool                  // If the migrations are disabled
		options                   []datastore.ClientOps // List of options
	}

	// modelOptions holds the model configuration
	modelOptions struct {
		migrateModelNames []string      // List of models for migration
		migrateModels     []interface{} // Models for migrations
		modelNames        []string      // List of all models
		models            []interface{} // Models for use in this engine
	}

	// newRelicOptions holds the configuration for NewRelic
	newRelicOptions struct {
		app     *newrelic.Application // NewRelic client application (if enabled)
		enabled bool                  // If NewRelic is enabled for deep Transaction tracing
	}

	// notificationsOptions holds the configuration for notifications
	notificationsOptions struct {
		notifications.ClientInterface                           // Notifications client
		options                       []notifications.ClientOps // List of options
		webhookEndpoint               string                    // Webhook endpoint
	}

	// paymailOptions holds the configuration for Paymail
	paymailOptions struct {
		client       paymail.ClientInterface // Paymail client for communicating with Paymail providers
		serverConfig *PaymailServerOptions   // Server configuration if Paymail is enabled
	}

	// PaymailServerOptions is the options for the Paymail server
	PaymailServerOptions struct {
		*server.Configuration                    // Server configuration if Paymail is enabled
		options               []server.ConfigOps // Options for the paymail server
		DefaultFromPaymail    string             // IE: from@domain.com
		DefaultNote           string             // IE: some note for address resolution
	}

	// taskManagerOptions holds the configuration for taskmanager
	taskManagerOptions struct {
		taskmanager.ClientInterface                          // Client for TaskManager
		cronTasks                   map[string]time.Duration // List of tasks and period times (IE: task_name 30*time.Minute = @every 30m)
		options                     []taskmanager.ClientOps  // List of options
	}
)

// NewClient creates a new client for all bux functionality
//
// If no options are given, it will use the defaultClientOptions()
// ctx may contain a NewRelic txn (or one will be created)
func NewClient(ctx context.Context, opts ...ClientOps) (ClientInterface, error) {

	// Create a new client with defaults
	client := &Client{options: defaultClientOptions()}

	// Overwrite defaults with any custom options provided by the user
	for _, opt := range opts {
		opt(client.options)
	}

	// Use NewRelic if it's enabled (use existing txn if found on ctx)
	ctx = client.GetOrStartTxn(ctx, "new_client")

	// Set the logger (if no custom logger was detected)
	if client.options.logger == nil {
		client.options.logger = zLogger.NewGormLogger(client.IsDebug(), 4)
	}

	// Load the Cachestore client
	var err error
	if err = client.loadCache(ctx); err != nil {
		return nil, err
	}

	// Load the cluster coordinator
	if err = client.loadCluster(ctx); err != nil {
		return nil, err
	}

	// Load the Datastore (automatically migrate models)
	if err = client.loadDatastore(ctx); err != nil {
		return nil, err
	}

	// Run custom model datastore migrations (after initializing models)
	if err = client.runModelMigrations(
		client.options.models.migrateModels...,
	); err != nil {
		return nil, err
	}

	// Load the Chainstate client
	if err = client.loadChainstate(ctx); err != nil {
		return nil, err
	}

	// Load the Paymail client (if client does not exist)
	if err = client.loadPaymailClient(); err != nil {
		return nil, err
	}

	// Load the Notification client (if client does not exist)
	if err = client.loadNotificationClient(); err != nil {
		return nil, err
	}

	// Load the Taskmanager (automatically start consumers and tasks)
	if err = client.loadTaskmanager(ctx); err != nil {
		return nil, err
	}

	// Register all model tasks & custom tasks
	if err = client.registerAllTasks(); err != nil {
		return nil, err
	}

	// Load the blockchain monitor
	if client.options.chainstate.Monitor() != nil {
		if err = client.loadMonitor(ctx); err != nil {
			return nil, err
		}
	}

	// Default paymail server config (generic capabilities and domain check disabled)
	if client.options.paymail.serverConfig.Configuration == nil {
		if err = client.loadDefaultPaymailConfig(); err != nil {
			return nil, err
		}
	}

	// Return the client
	return client, nil
}

// AddModels will add additional models to the client
func (c *Client) AddModels(ctx context.Context, autoMigrate bool, models ...interface{}) error {

	// Store the models locally in the client
	c.options.addModels(modelList, models...)

	// Should we migrate the models?
	if autoMigrate {

		// Ensure we have a datastore
		d := c.Datastore()
		if d == nil {
			return ErrDatastoreRequired
		}

		// Apply the database migration with the new models
		if err := d.AutoMigrateDatabase(ctx, models...); err != nil {
			return err
		}

		// Add to the list
		c.options.addModels(migrateList, models...)

		// Run model migrations
		if err := c.runModelMigrations(models...); err != nil {
			return err
		}
	}

	// Register all tasks (again)
	return c.registerAllTasks()
}

// Cachestore will return the Cachestore IF: exists and is enabled
func (c *Client) Cachestore() cachestore.ClientInterface {
	if c.options.cacheStore != nil && c.options.cacheStore.ClientInterface != nil {
		return c.options.cacheStore.ClientInterface
	}
	return nil
}

// Cluster will return the cluster coordinator client
func (c *Client) Cluster() cluster.ClientInterface {
	if c.options.cluster != nil && c.options.cluster.ClientInterface != nil {
		return c.options.cluster.ClientInterface
	}
	return nil
}

// Chainstate will return the Chainstate service IF: exists and is enabled
func (c *Client) Chainstate() chainstate.ClientInterface {
	if c.options.chainstate != nil && c.options.chainstate.ClientInterface != nil {
		return c.options.chainstate.ClientInterface
	}
	return nil
}

// Close will safely close any open connections (cache, datastore, etc.)
func (c *Client) Close(ctx context.Context) error {

	if txn := newrelic.FromContext(ctx); txn != nil {
		defer txn.StartSegment("close_all").End()
	}

	// If we loaded a Monitor, remove the long-lasting lock-key before closing cachestore
	cs := c.Cachestore()
	m := c.Chainstate().Monitor()
	if m != nil && cs != nil && len(m.GetLockID()) > 0 {
		_ = cs.Delete(ctx, fmt.Sprintf(lockKeyMonitorLockID, m.GetLockID()))
	}

	// Close Cachestore
	if cs != nil {
		cs.Close(ctx)
		c.options.cacheStore.ClientInterface = nil
	}

	// Close Chainstate
	ch := c.Chainstate()
	if ch != nil {
		ch.Close(ctx)
		c.options.chainstate.ClientInterface = nil
	}

	// Close Datastore
	ds := c.Datastore()
	if ds != nil {
		if err := ds.Close(ctx); err != nil {
			return err
		}
		c.options.dataStore.ClientInterface = nil
	}

	// Close Taskmanager
	tm := c.Taskmanager()
	if tm != nil {
		if err := tm.Close(ctx); err != nil {
			return err
		}
		c.options.taskManager.ClientInterface = nil
	}
	return nil
}

// Datastore will return the Datastore if it exists
func (c *Client) Datastore() datastore.ClientInterface {
	if c.options.dataStore != nil && c.options.dataStore.ClientInterface != nil {
		return c.options.dataStore.ClientInterface
	}
	return nil
}

// Debug will toggle the debug mode (for all resources)
func (c *Client) Debug(on bool) {

	// Set the flag on the current client
	c.options.debug = on

	// Set debugging on the Cachestore
	if cs := c.Cachestore(); cs != nil {
		cs.Debug(on)
	}

	// Set debugging on the Chainstate
	if ch := c.Chainstate(); ch != nil {
		ch.Debug(on)
	}

	// Set debugging on the Datastore
	if ds := c.Datastore(); ds != nil {
		ds.Debug(on)
	}

	// Set debugging on the Notifications
	if n := c.Notifications(); n != nil {
		n.Debug(on)
	}

	// Set debugging on the Taskmanager
	if tm := c.Taskmanager(); tm != nil {
		tm.Debug(on)
	}
}

// DefaultSyncConfig will return the default sync config from the client defaults (for chainstate)
func (c *Client) DefaultSyncConfig() *SyncConfig {
	return &SyncConfig{
		Broadcast:        c.options.chainstate.broadcasting,
		BroadcastInstant: c.options.chainstate.broadcastInstant,
		PaymailP2P:       c.options.chainstate.paymailP2P,
		SyncOnChain:      c.options.chainstate.syncOnChain,
	}
}

// EnableNewRelic will enable NewRelic tracing
func (c *Client) EnableNewRelic() {
	if c.options.newRelic != nil && c.options.newRelic.app != nil {
		c.options.newRelic.enabled = true
	}
}

// GetOrStartTxn will check for an existing NewRelic transaction, if not found, it will make a new transaction
func (c *Client) GetOrStartTxn(ctx context.Context, name string) context.Context {
	if c.IsNewRelicEnabled() && c.options.newRelic.app != nil {
		txn := newrelic.FromContext(ctx)
		if txn == nil {
			txn = c.options.newRelic.app.StartTransaction(name)
		}
		ctx = newrelic.NewContext(ctx, txn)
	}
	return ctx
}

// GetTaskPeriod will return the period for a given task name
func (c *Client) GetTaskPeriod(name string) time.Duration {
	if d, ok := c.options.taskManager.cronTasks[name]; ok {
		return d
	}
	return 0
}

// GetModelNames will return the model names that have been loaded
func (c *Client) GetModelNames() []string {
	return c.options.models.modelNames
}

// HTTPClient will return the http interface to use in the client
func (c *Client) HTTPClient() HTTPInterface {
	return c.options.httpClient
}

// ImportBlockHeadersFromURL will the URL where to import block headers from
func (c *Client) ImportBlockHeadersFromURL() string {
	return c.options.importBlockHeadersURL
}

// IsDebug will return the debug flag (bool)
func (c *Client) IsDebug() bool {
	return c.options.debug
}

// IsNewRelicEnabled will return the flag (bool)
func (c *Client) IsNewRelicEnabled() bool {
	return c.options.newRelic.enabled
}

// IsMempoolMonitoringEnabled will return whether mempool monitoring is on
func (c *Client) IsMempoolMonitoringEnabled() bool {
	return c.options.chainstate.IsNewRelicEnabled()
}

// IsITCEnabled will return the flag (bool)
func (c *Client) IsITCEnabled() bool {
	return c.options.itc
}

// IsIUCEnabled will return the flag (bool)
func (c *Client) IsIUCEnabled() bool {
	return c.options.iuc
}

// IsEncryptionKeySet will return the flag (bool) if the encryption key has been set
func (c *Client) IsEncryptionKeySet() bool {
	return len(c.options.encryptionKey) > 0
}

// IsMigrationEnabled will return the flag (bool)
func (c *Client) IsMigrationEnabled() bool {
	return !c.options.dataStore.migrationDisabled
}

// Logger will return the Logger if it exists
func (c *Client) Logger() zLogger.GormLoggerInterface {
	return c.options.logger
}

// ModifyTaskPeriod will modify a cron task's duration period from the default
func (c *Client) ModifyTaskPeriod(name string, period time.Duration) error {

	// Basic validation on parameters
	if len(name) == 0 {
		return taskmanager.ErrMissingTaskName
	} else if period <= 0 {
		return taskmanager.ErrInvalidTaskDuration
	}

	// Ensure task manager has been loaded
	if c.Taskmanager() == nil || c.options.taskManager.cronTasks == nil {
		return ErrTaskManagerNotLoaded
	} else if len(c.options.taskManager.cronTasks) == 0 {
		return taskmanager.ErrNoTasksFound
	}

	// Check for the task
	if d, ok := c.options.taskManager.cronTasks[name]; !ok {
		return taskmanager.ErrTaskNotFound
	} else if d == period {
		return nil
	}

	// Set the new period on the client
	c.options.taskManager.cronTasks[name] = period

	// register all tasks again (safely override)
	return c.registerAllTasks()
}

// Notifications will return the Notifications if it exists
func (c *Client) Notifications() notifications.ClientInterface {
	if c.options.notifications != nil && c.options.notifications.ClientInterface != nil {
		return c.options.notifications.ClientInterface
	}
	return nil
}

// SetNotificationsClient will overwrite the notification's client with the given client
func (c *Client) SetNotificationsClient(client notifications.ClientInterface) {
	c.options.notifications.ClientInterface = client
}

// Taskmanager will return the Taskmanager if it exists
func (c *Client) Taskmanager() taskmanager.ClientInterface {
	if c.options.taskManager != nil && c.options.taskManager.ClientInterface != nil {
		return c.options.taskManager.ClientInterface
	}
	return nil
}

// UserAgent will return the user agent
func (c *Client) UserAgent() string {
	return c.options.userAgent
}

// Version will return the version
func (c *Client) Version() string {
	return version
}
