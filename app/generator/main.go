package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"top-queries/internal/models"

	"github.com/segmentio/kafka-go"
)

// MarketQueries holds organic search terms representing standard marketplace categories.
var marketQueries = []string{
	"чехол на iphone 13", "чехол на iphone 14", "наушники беспроводные", "пауэрбанк быстрый",
	"кабель type-c", "смарт часы мужские", "мышь беспроводная", "клавиатура механика",
	"стекло на айфон", "зарядка для телефона", "штатив для телефона", "колонки для компьютера",

	"футболка мужская оверсайз", "носки белые хлопковые", "кроссовки летние", "сумка женская кожаная",
	"шорты спортивные", "платье летнее шелковое", "кепка мужская", "джинсы широкие",
	"худи черное", "купальник раздельный", "тапочки резиновые", "рюкзак городской",

	"постельное белье дуэт", "шторы блэкаут", "полотенце махровое", "свеча ароматическая",
	"организатор для косметики", "коврик для ванной", "кружка керамическая", "вешалки для одежды",

	"крем для лица увлажняющий", "патчи для глаз", "масло для волос", "сыворотка с витамином С",
	"протеин сывороточный", "креатин моногидрат", "коврик для йоги", "бутылка для воды",

	"кофе в зернах 1кг", "чай листовой зеленый", "арахисовая паста", "кокосовое молоко",
}

// FraudTargetQueries holds specific queries used to simulate SEO manipulation and ranking fraud.
var fraudTargetQueries = []string{
	"купить кроссовки abibas со скидкой",
	"лучший крем ромашка от ИП Пупкин",
	"оригинальные наушники подслушники купить дешево",
}

// DirtyQueries holds restricted terms used to validate stop-list filter components.
var dirtyQueries = []string{
	"скам", "накрутка отзывов вб", "вейп электронка", "сигареты оптом",
	"купить паспорт", "хакерский софт", "курительные смеси",
}

func main() {
	broker := flag.String("broker", "localhost:9092", "Kafka broker address")
	topic := flag.String("topic", "wb-search-queries", "Kafka topic name")
	targetRPS := flag.Int("rps", 100, "Target requests per second")
	flag.Parse()

	log.Printf("Starting High-Load generator to %s/%s at %d RPS...", *broker, *topic, *targetRPS)

	writer := &kafka.Writer{
		Addr:         kafka.TCP(*broker),
		Topic:        *topic,
		Balancer:     &kafka.LeastBytes{},
		Async:        true,
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
	}
	defer writer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tickerDuration := time.Second / time.Duration(*targetRPS)
	ticker := time.NewTicker(tickerDuration)
	defer ticker.Stop()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		select {
		case <-ctx.Done():
			log.Println("Generator stopped by system signal")
			return
		case <-ticker.C:
			var query string
			var ip string
			var userID string
			dice := rng.Intn(100)

			switch {
			case dice < 3:
				query = dirtyQueries[rng.Intn(len(dirtyQueries))]
				ip = generateRandomIP(rng, "192.168")
				userID = fmt.Sprintf("user_anon_%d", rng.Intn(100000))

			case dice >= 3 && dice < 10:
				query = fraudTargetQueries[rng.Intn(len(fraudTargetQueries))]
				botIPs := []string{"66.66.66.66", "77.77.77.77", "88.88.88.88"}
				ip = botIPs[rng.Intn(len(botIPs))]
				userID = fmt.Sprintf("bot_user_%d", rng.Intn(100000))

			default:
				query = getZipfQuery(rng, marketQueries)
				ip = generateRandomIP(rng, "172.20")
				userID = fmt.Sprintf("user_id_%d", rng.Intn(500000))
			}

			event := models.SearchEvent{
				Query:     query,
				UserID:    userID,
				IP:        ip,
				Timestamp: time.Now().UTC().Unix(),
			}

			payload, err := json.Marshal(event)
			if err != nil {
				log.Printf("failed to marshal event: %v", err)
				continue
			}

			err = writer.WriteMessages(ctx, kafka.Message{
				Key:   []byte(ip),
				Value: payload,
			})
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Printf("failed to write message to kafka: %v", err)
			}
		}
	}
}

// generateRandomIP creates a pseudorandom IPv4 address string within the specified subnet prefix.
func generateRandomIP(rng *rand.Rand, prefix string) string {
	return fmt.Sprintf("%s.%d.%d", prefix, rng.Intn(254)+1, rng.Intn(254)+1)
}

// getZipfQuery selects a query from the slice biasing density towards the lower indices
// to mathematically approximate Zipf's law distribution of human search behavior.
func getZipfQuery(rng *rand.Rand, queries []string) string {
	n := len(queries)
	if n == 0 {
		return ""
	}

	factor := 1.0 - rng.Float64()*rng.Float64()
	idx := int(float64(n) * (1.0 - factor))

	if idx >= n {
		idx = n - 1
	}
	return queries[idx]
}
