package main

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/jpeg"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/nfnt/resize"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	// MongoDB connection setup
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	// Select the database and collection
	collection := client.Database("podgo").Collection("podcasts")

	// Define the projection to only include the podlistUrl and image fields
	projection := bson.D{
		{Key: "podlistUrl", Value: 1},
		{Key: "image", Value: 1},
	}

	// Find all documents in the collection with the specified projection
	cur, err := collection.Find(context.TODO(), bson.D{}, options.Find().SetProjection(projection))
	if err != nil {
		log.Fatalf("Failed to find documents: %v", err)
	}
	defer cur.Close(context.TODO())

	// Ensure the target directory exists
	if err := os.MkdirAll("img", os.ModePerm); err != nil {
		log.Fatalf("Failed to create directory: %v", err)
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, 10) // Limit to 10 concurrent goroutines

	for cur.Next(context.TODO()) {
		var podcast struct {
			PodlistUrl string `bson:"podlistUrl"`
			Image      string `bson:"image"`
		}

		err := cur.Decode(&podcast)
		if err != nil {
			log.Printf("Failed to decode document: %v", err)
			continue
		}

		wg.Add(1)
		sem <- struct{}{} // Acquire a token
		go func(podcast struct {
			PodlistUrl string `bson:"podlistUrl"`
			Image      string `bson:"image"`
		}) {
			defer wg.Done()
			defer func() { <-sem }() // Release the token

			// Download the image
			imageData, err := downloadImage(podcast.Image)
			if err != nil {
				log.Printf("Failed to download image: %s, error: %v", podcast.Image, err)
				return
			}

			// Resize the image
			resizedImg, err := resizeImage(imageData, 800, 800) // Resize to 800x800
			if err != nil {
				log.Printf("Failed to resize image: %v", err)
				return
			}

			// Save the image as a JPEG
			err = saveImage(resizedImg, filepath.Join("img", podcast.PodlistUrl+".jpg"))
			if err != nil {
				log.Printf("Failed to save image: %v", err)
			}
		}(podcast)
	}

	if err := cur.Err(); err != nil {
		log.Fatalf("Cursor error: %v", err)
	}

	wg.Wait()
	fmt.Println("All images processed.")
}

func downloadImage(url string) (image.Image, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to get image: %w", err)
	}
	defer resp.Body.Close()

	imgData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read image data: %w", err)
	}

	img, _, err := image.Decode(bytes.NewReader(imgData))
	if err != nil {
		return nil, fmt.Errorf("failed to decode image: %w", err)
	}
	return img, nil
}

func resizeImage(img image.Image, width, height uint) (image.Image, error) {
	return resize.Resize(width, height, img, resize.Lanczos3), nil
}

func saveImage(img image.Image, path string) error {
	out, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer out.Close()

	options := &jpeg.Options{Quality: 75} // Optimize JPEG
	return jpeg.Encode(out, img, options)
}
