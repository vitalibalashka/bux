package bux

import (
	"context"
	"errors"
	"time"

	"github.com/BuxOrg/bux/utils"
	"github.com/mrz1836/go-datastore"
)

// GetPaymailAddress will get a paymail address model
func (c *Client) GetPaymailAddress(ctx context.Context, address string, opts ...ModelOps) (*PaymailAddress, error) {

	// Check for existing NewRelic transaction
	ctx = c.GetOrStartTxn(ctx, "get_paymail_address")

	// Get the paymail address
	paymailAddress, err := getPaymailAddress(ctx, address, append(opts, c.DefaultModelOptions()...)...)
	if err != nil {
		return nil, err
	} else if paymailAddress == nil {
		return nil, ErrMissingPaymail
	}

	return paymailAddress, nil
}

// GetPaymailAddresses will get all the paymail addresses from the Datastore
func (c *Client) GetPaymailAddresses(ctx context.Context, metadataConditions *Metadata,
	conditions *map[string]interface{}, queryParams *datastore.QueryParams, opts ...ModelOps) ([]*PaymailAddress, error) {

	// Check for existing NewRelic transaction
	ctx = c.GetOrStartTxn(ctx, "get_paymail_addresses")

	// Get the paymail address
	paymailAddresses, err := getPaymailAddresses(
		ctx, metadataConditions, conditions, queryParams,
		c.DefaultModelOptions(opts...)...,
	)
	if err != nil {
		return nil, err
	}

	return paymailAddresses, nil
}

// GetPaymailAddressesCount will get a count of all the paymail addresses from the Datastore
func (c *Client) GetPaymailAddressesCount(ctx context.Context, metadataConditions *Metadata,
	conditions *map[string]interface{}, opts ...ModelOps) (int64, error) {

	// Check for existing NewRelic transaction
	ctx = c.GetOrStartTxn(ctx, "get_paymail_addresses_count")

	// Get the paymail address
	count, err := getPaymailAddressesCount(
		ctx, metadataConditions, conditions,
		c.DefaultModelOptions(opts...)...,
	)
	if err != nil {
		return 0, err
	}

	return count, nil
}

// GetPaymailAddressesByXPubID will get all the paymail addresses for an xPubID from the Datastore
func (c *Client) GetPaymailAddressesByXPubID(ctx context.Context, xPubID string, metadataConditions *Metadata,
	conditions *map[string]interface{}, queryParams *datastore.QueryParams) ([]*PaymailAddress, error) {

	// Check for existing NewRelic transaction
	ctx = c.GetOrStartTxn(ctx, "get_paymail_by_xpub")

	if conditions == nil {
		*conditions = make(map[string]interface{})
	}
	// add the xpub_id to the conditions
	(*conditions)["xpub_id"] = xPubID

	// Get the paymail address
	paymailAddresses, err := getPaymailAddresses(
		ctx, metadataConditions, conditions, queryParams,
		c.DefaultModelOptions()...,
	)
	if err != nil {
		return nil, err
	}

	return paymailAddresses, nil
}

// NewPaymailAddress will create a new paymail address
func (c *Client) NewPaymailAddress(ctx context.Context, xPubKey, address, publicName, avatar string,
	opts ...ModelOps) (*PaymailAddress, error) {

	// Check for existing NewRelic transaction
	ctx = c.GetOrStartTxn(ctx, "new_paymail_address")

	// Get the xPub (make sure it exists)
	_, err := getXpubWithCache(ctx, c, xPubKey, "", c.DefaultModelOptions()...)
	if err != nil {
		return nil, err
	}

	// Check if the paymail address already exists
	paymail, err := getPaymailAddress(ctx, address, opts...)
	if paymail != nil {
		return nil, errors.New("paymail address already exists")
	}
	if err != nil {
		return nil, err
	}

	// Start the new paymail address model
	paymailAddress := newPaymail(
		address,
		append(opts, c.DefaultModelOptions(
			New(),
			WithXPub(xPubKey),
		)...)...,
	)

	// Set the optional fields
	paymailAddress.Avatar = avatar
	paymailAddress.PublicName = publicName

	// Save the model
	if err = paymailAddress.Save(ctx); err != nil {
		return nil, err
	}
	return paymailAddress, nil
}

// DeletePaymailAddress will delete a paymail address
func (c *Client) DeletePaymailAddress(ctx context.Context, address string, opts ...ModelOps) error {

	// Check for existing NewRelic transaction
	ctx = c.GetOrStartTxn(ctx, "delete_paymail_address")

	// Get the paymail address
	paymailAddress, err := getPaymailAddress(ctx, address, append(opts, c.DefaultModelOptions()...)...)
	if err != nil {
		return err
	} else if paymailAddress == nil {
		return ErrMissingPaymail
	}

	// todo: make a better approach for deleting paymail addresses?
	var randomString string
	if randomString, err = utils.RandomHex(16); err != nil {
		return err
	}

	// We will do a soft delete to make sure we still have the history for this address
	// setting the Domain to a random string solved the problem of the unique index on Alias/Domain
	// todo: figure out a different approach - history table?
	paymailAddress.Alias = paymailAddress.Alias + "@" + paymailAddress.Domain
	paymailAddress.Domain = randomString
	paymailAddress.DeletedAt.Valid = true
	paymailAddress.DeletedAt.Time = time.Now()

	return paymailAddress.Save(ctx)
}

// UpdatePaymailAddressMetadata will update the metadata in an existing paymail address
func (c *Client) UpdatePaymailAddressMetadata(ctx context.Context, address string,
	metadata Metadata, opts ...ModelOps) (*PaymailAddress, error) {

	// Check for existing NewRelic transaction
	ctx = c.GetOrStartTxn(ctx, "update_paymail_address_metadata")

	// Get the paymail address
	paymailAddress, err := getPaymailAddress(ctx, address, append(opts, c.DefaultModelOptions()...)...)
	if err != nil {
		return nil, err
	} else if paymailAddress == nil {
		return nil, ErrMissingPaymail
	}

	// Update the metadata
	paymailAddress.UpdateMetadata(metadata)

	// Save the model
	if err = paymailAddress.Save(ctx); err != nil {
		return nil, err
	}

	return paymailAddress, nil
}

// UpdatePaymailAddress will update optional fields of the paymail address
func (c *Client) UpdatePaymailAddress(ctx context.Context, address, publicName, avatar string,
	opts ...ModelOps) (*PaymailAddress, error) {

	// Check for existing NewRelic transaction
	ctx = c.GetOrStartTxn(ctx, "update_paymail_address")

	// Get the paymail address
	paymailAddress, err := getPaymailAddress(ctx, address, append(opts, c.DefaultModelOptions()...)...)
	if err != nil {
		return nil, err
	} else if paymailAddress == nil {
		return nil, ErrMissingPaymail
	}

	// Update the public name
	if paymailAddress.PublicName != publicName {
		paymailAddress.PublicName = publicName
	}

	// Update the avatar
	if paymailAddress.Avatar != avatar {
		paymailAddress.Avatar = avatar
	}

	// Save the model
	if err = paymailAddress.Save(ctx); err != nil {
		return nil, err
	}

	return paymailAddress, nil
}
