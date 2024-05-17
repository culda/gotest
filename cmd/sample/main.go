package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Balance represents a user's balance with available amount
type Balance struct {
	UserID    string  `json:"user_id"`
	Available float64 `json:"available"`
	Total     float64 `json:"total"`
}

// Order represents an order in the Orders table
type Order struct {
	OrderID string  `json:"order_id"`
	UserID  string  `json:"user_id"`
	Amount  float64 `json:"amount"`
	Status  string  `json:"status"`
}

// FetchBalance fetches the balance from DynamoDB
func FetchBalance(svc *dynamodb.DynamoDB, userID string) (*Balance, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String("Balances"),
		Key: map[string]*dynamodb.AttributeValue{
			"user_id": {
				S: aws.String(userID),
			},
		},
	}

	result, err := svc.GetItem(input)
	if err != nil {
		return nil, fmt.Errorf("failed to get item from DynamoDB: %v", err)
	}
	if result.Item == nil {
		return nil, fmt.Errorf("no item found with the given user_id: %s", userID)
	}

	balance := new(Balance)
	err = dynamodbattribute.UnmarshalMap(result.Item, balance)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal result item: %v", err)
	}

	return balance, nil
}

// UpdateBalance updates a user's balance in the DynamoDB table with concurrency safety and negative amount checks
func UpdateBalance(svc *dynamodb.DynamoDB, userID string, availableChange, totalChange float64) error {
	for {
		balance, err := FetchBalance(svc, userID)
		if err != nil {
			return err
		}

		newAvailable := balance.Available + availableChange
		newTotal := balance.Total + totalChange
		if newAvailable < 0 || newTotal < 0 {
			return fmt.Errorf("update would result in negative balance")
		}

		input := &dynamodb.UpdateItemInput{
			TableName: aws.String("Balances"),
			Key: map[string]*dynamodb.AttributeValue{
				"user_id": {
					S: aws.String(userID),
				},
			},
			UpdateExpression:    aws.String("set available = :newAvailable, total = :newTotal"),
			ConditionExpression: aws.String("available = :currentAvailable AND total = :currentTotal"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":newAvailable": {
					N: aws.String(fmt.Sprintf("%f", newAvailable)),
				},
				":newTotal": {
					N: aws.String(fmt.Sprintf("%f", newTotal)),
				},
				":currentAvailable": {
					N: aws.String(fmt.Sprintf("%f", balance.Available)),
				},
				":currentTotal": {
					N: aws.String(fmt.Sprintf("%f", balance.Total)),
				},
			},
		}

		_, err = svc.UpdateItem(input)
		if err != nil {
			if isConditionalCheckFailed(err) {
				continue
			}
			return fmt.Errorf("failed to update item in DynamoDB: %v", err)
		}

		return nil
	}
}

// isConditionalCheckFailed checks if the error is a ConditionalCheckFailedException
func isConditionalCheckFailed(err error) bool {
	if aerr, ok := err.(awserr.Error); ok && aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
		return true
	}
	return false
}

// CreateSellOrder creates a new sell order and updates the user's balance
func CreateSellOrder(svc *dynamodb.DynamoDB, userID string, orderID string, amount float64) error {
	if amount <= 0 {
		return fmt.Errorf("amount must be positive")
	}

	order := &Order{
		OrderID: orderID,
		UserID:  userID,
		Amount:  amount,
		Status:  "Pending",
	}

	av, err := dynamodbattribute.MarshalMap(order)
	if err != nil {
		return fmt.Errorf("failed to marshal order: %v", err)
	}

	putOrderInput := &dynamodb.PutItemInput{
		TableName: aws.String("Orders"),
		Item:      av,
	}

	_, err = svc.PutItem(putOrderInput)
	if err != nil {
		return fmt.Errorf("failed to put order in DynamoDB: %v", err)
	}

	err = UpdateBalance(svc, userID, -amount, -amount)
	if err != nil {
		return fmt.Errorf("failed to update balance: %v", err)
	}

	return nil
}

// Settle settles an order and updates the user's balance
func Settle(svc *dynamodb.DynamoDB, orderID string) error {
	// Fetch the order
	getOrderInput := &dynamodb.GetItemInput{
		TableName: aws.String("Orders"),
		Key: map[string]*dynamodb.AttributeValue{
			"order_id": {
				S: aws.String(orderID),
			},
		},
	}

	orderResult, err := svc.GetItem(getOrderInput)
	if err != nil {
		return fmt.Errorf("failed to get order from DynamoDB: %v", err)
	}
	if orderResult.Item == nil {
		return fmt.Errorf("no order found with the given order_id: %s", orderID)
	}

	order := new(Order)
	err = dynamodbattribute.UnmarshalMap(orderResult.Item, order)
	if err != nil {
		return fmt.Errorf("failed to unmarshal order: %v", err)
	}

	if order.Status == "Settled" {
		return fmt.Errorf("order is already settled")
	}

	// Update the order status to Settled
	updateOrderInput := &dynamodb.UpdateItemInput{
		TableName: aws.String("Orders"),
		Key: map[string]*dynamodb.AttributeValue{
			"order_id": {
				S: aws.String(orderID),
			},
		},
		UpdateExpression: aws.String("set status = :newStatus"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":newStatus": {
				S: aws.String("Settled"),
			},
		},
	}

	_, err = svc.UpdateItem(updateOrderInput)
	if err != nil {
		return fmt.Errorf("failed to update order status in DynamoDB: %v", err)
	}

	return nil
}
