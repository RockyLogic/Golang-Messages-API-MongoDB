package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Message struct {
    ID        primitive.ObjectID `bson:"_id,omitempty"`
    Recipient string             `bson:"recipient"`
    Sender    string             `bson:"sender"`
    Content   string             `bson:"content"`
    Timestamp time.Time          `bson:"timestamp"`
}

var logger *zap.Logger

// curl -i -X GET http://localhost:8080/messages
func getMessages(collection *mongo.Collection) func(c *gin.Context) {
    return func(c *gin.Context) {

        logger.Info(c.Request.URL.Path)

        // Create a context for the database operation
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()

        // Fetch all messages from the collection
        cursor, err := collection.Find(ctx, bson.D{})
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve messages"})
            return
        }
        defer cursor.Close(ctx)

        // Store the messages in a slice
        var messages []Message = []Message{}
        if err := cursor.All(ctx, &messages); err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decode messages"})
            return
        }

        c.JSON(http.StatusOK, messages)
        logger.Info("Messages retrieved")
    }
}

// curl -i -X GET http://localhost:8080/messages/64bd837566b7829eaa7ea650
func getMessageByID(collection *mongo.Collection) func(c *gin.Context) {
    return func(c *gin.Context) {

        logger.Info(c.Request.URL.Path)

        // Create a context for the database operation
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()

        var message Message

        // Parse the message ID to MongoDB ObjectID
        messageID := c.Param("id")
        objectID, err := primitive.ObjectIDFromHex(string(messageID))
        if err != nil {
            c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid message ID"})
            logger.Fatal("Invalid message ID")
            return
        }

        err = collection.FindOne(ctx, bson.M{"_id": objectID}).Decode(&message)
        if err != nil {
            if err == mongo.ErrNoDocuments {
                c.JSON(http.StatusNotFound, gin.H{"error": "Message not found"})
                logger.Fatal("Message not found")
            } else {
                c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to find message"})
                logger.Fatal("Failed to find message")
            }
            return
        }
        c.JSON(http.StatusOK, message)
        logger.Info(fmt.Sprintf("Message %s fetched", messageID))
    }
}

// curl -i -X POST -H "Content-Type: application/json" -d '{"recipient":"Alice","sender":"Bob","content":"Hello, Alice!"}' http://localhost:8080/messages
func sendMessage(collection *mongo.Collection) func(c *gin.Context) {
    return func(c *gin.Context) {

        logger.Info(c.Request.URL.Path)

        // Create a context for the database operation
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()

        // Create a message object from the request body
        var message Message
        if err := c.BindJSON(&message); err != nil {
            c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to decode request body"})
            logger.Fatal("Failed to decode request body")
            return
        }

        // Insert the message into the collection
        result, err := collection.InsertOne(ctx, message)
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to insert message"})
            logger.Fatal("Failed to insert message")
            return
        }

        // Return the ID of the inserted document
        c.JSON(http.StatusOK, result.InsertedID)
        logger.Info(fmt.Sprintf("Message %s sent", result.InsertedID))
    }
}

// curl -i -X PUT -H "Content-Type: application/json" -d '{"recipient":"Alice","sender":"Bob","content":"Hello, Bob!"}' http://localhost:8080/messages/64bd83ba66b7829eaa7ea651
func updateMessage(collection *mongo.Collection) func(c *gin.Context) {
    return func(c *gin.Context) {

        logger.Info(c.Request.URL.Path)

        // Create a context for the database operation
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()

        // Parse the message ID to MongoDB ObjectID
        messageID := c.Param("id")
        objectID, err := primitive.ObjectIDFromHex(string(messageID))
        if err != nil {
            c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid message ID"})
            logger.Fatal("Invalid message ID")
            return
        }

        // Parse the updated message data from the request body
        var updatedMessage Message
        if err := c.ShouldBindJSON(&updatedMessage); err != nil {
            c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid message data"})
            logger.Fatal("Invalid message data")
            return
        }

        // Set the timestamp & ID for the updated message
        updatedMessage.Timestamp = time.Now()
        updatedMessage.ID = objectID

        // Perform the update by replacing the existing message with the updated message
        res, err := collection.ReplaceOne(ctx, bson.M{"_id": objectID}, updatedMessage)
        if err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update message"})
            logger.Fatal("Failed to update message")
            return
        }
    
        if res.MatchedCount == 0 {
            c.JSON(http.StatusNotFound, gin.H{"error": "Message not found"})
            logger.Fatal("Message not found")
            return
        }

        c.JSON(http.StatusOK, gin.H{"message": "Message updated successfully", "updatedMessage": updatedMessage})
        logger.Info(fmt.Sprintf("Message %s updated", messageID))
    }
}

// curl -i -X DELETE http://localhost:8080/messages/64bd85a4caedb30692d69de0
func deleteMessageById(collection *mongo.Collection) func(c *gin.Context) {
    return func(c *gin.Context) {
        
        logger.Info(c.Request.URL.Path)

        // Create a context for the database operation
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()

        var message Message

        // Parse the message ID to MongoDB ObjectID
        messageID := c.Param("id")
        objectID, err := primitive.ObjectIDFromHex(string(messageID))
        if err != nil {
            c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid message ID"})
            logger.Fatal("Invalid message ID")
            return
        }

        err = collection.FindOneAndDelete(ctx, bson.M{"_id": objectID}).Decode(&message)
        if err != nil {
            if err == mongo.ErrNoDocuments {
                c.JSON(http.StatusNotFound, gin.H{"error": "Message not found"})
                logger.Fatal("Message not found")
            } else {
                c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to find message"})
                logger.Fatal("Failed to find message")
            }
            return
        }

        c.JSON(http.StatusOK, message)
        logger.Info(fmt.Sprintf("Message %s deleted", messageID))
    }
}

func loggerSetup() (*zap.Logger, error) {
    // Logger setup
    loggerConfig := zap.NewProductionConfig()
    loggerConfig.EncoderConfig.TimeKey = "timestamp"
    loggerConfig.EncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout(time.RFC3339)

    logger, err := loggerConfig.Build()
    if err != nil {
        log.Fatal(err)
        return nil, err
    }

    return logger, nil
}

func setupMongoDB() (*mongo.Collection, error){
    
    // Context for MongoDB connection
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    // MongoDB connection
    connectionString := "mongodb://localhost:27017"
    client, err := mongo.Connect(ctx, options.Client().ApplyURI(connectionString))
    if err != nil {
        fmt.Println("Error connecting to MongoDB:", err)
        return nil, err
    }

    // MongoDb Ping
    err = client.Ping(ctx, nil)
    if err != nil {
        fmt.Println("Failed to ping MongoDB:", err)
        return nil, err
    }

    collection := client.Database("Golang").Collection("messages")

    return collection, nil
}

func main() {
    // Logger setup
    logger, err := loggerSetup()
    if err != nil {
        logger.Fatal("Error setting up logger: " + err.Error())
    }
    logger.Info("Setup Complete: Logger")

    // MongoDB setup
    collection, err := setupMongoDB()
    if err != nil {
        logger.Fatal("Error setting up MongoDB:" + err.Error())
    }
    logger.Info("Setup Complete: MongoDB")

    router := gin.Default()
    router.GET("/messages", getMessages(collection))
    router.GET("/messages/:id", getMessageByID(collection))
    router.POST("/messages", sendMessage(collection))
    router.PATCH("/messages/:id", updateMessage(collection))
    router.DELETE("/messages/:id", deleteMessageById(collection))

    router.Run("localhost:8080")
}