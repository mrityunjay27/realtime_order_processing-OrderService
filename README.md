# Order Service

The **orchestrator** of the Realtime Order Processing System. This service receives customer orders, coordinates inventory reservation and payment processing through Kafka events, and manages the order lifecycle using the saga pattern with compensation.

## What It Does

- **Creates orders** via REST API (`POST /api/orders/`)
- **Orchestrates the saga**: triggers inventory reservation, then payment processing
- **Handles compensation**: if payment fails, releases reserved inventory
- **Manages order state transitions**: `PENDING → INVENTORY_RESERVED → COMPLETED` (or `FAILED`)
- **Publishes events** to Kafka via the transactional outbox pattern
- **Consumes events** from Inventory and Payment services to drive order state

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Order Service (:8001)                     │
├─────────────────────────────────────────────────────────────┤
│  Web API (DRF)  │  Kafka Consumer  │  Outbox Publisher      │
│       │         │        │         │        │                │
│       ▼         │        ▼         │        ▼                │
│  OrderService   │  EventHandlers   │  OutboxService         │
│       │         │        │         │        │                │
│       ▼         │        ▼         │        ▼                │
│  OutboxEvent ───────────────────────────────────────────────│
│  EventHistory │  ProcessedEvents (idempotency)              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    Kafka (orders.created, payments.requested, inventory.release)
                              │
            ┌─────────────────┼─────────────────┐
            ▼                 ▼                 ▼
     Inventory Svc      Payment Svc       (retry/DLQ topics)
```

## How to Start

### Prerequisites
- Python 3.12+
- PostgreSQL (via Docker)
- Kafka infrastructure (via Docker)

### 1. Start Dependencies

```bash
# Start Kafka infrastructure (from project root)
cd infra && docker compose up -d

# Start PostgreSQL for this service
cd order_service && docker compose up -d
```

### 2. Setup Environment

```bash
cd order_service
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Run Migrations

```bash
python manage.py migrate
```

### 4. Start the Service

**Option A: All processes via honcho (recommended)**
```bash
honcho start
```
This starts three processes: web (API), consumer (Kafka), publisher (outbox).

**Option B: Individual processes**
```bash
# Terminal 1: Web API
python manage.py runserver 0.0.0.0:8001

# Terminal 2: Kafka Consumer
python manage.py consume_events

# Terminal 3: Outbox Publisher
python manage.py publish_outbox
```

**Option C: VS Code Launch Configs**
Use the provided launch configurations for debugging.

## Dependencies on Other Services

| Dependency | Type | Purpose |
|------------|------|---------|
| **Inventory Service** | Kafka consumer | Reserves stock when order is created |
| **Payment Service** | Kafka consumer | Processes payment after inventory reservation |
| **Kafka** | Message broker | Asynchronous communication between services |
| **PostgreSQL** | Database | Persistent storage for orders, events |
| **Grafana/Loki** | Observability | Log aggregation and monitoring |

### Event Flow

1. **Publishes** `orders.created` → Inventory Service
2. **Consumes** `inventory.reserved` → Triggers payment request
3. **Consumes** `inventory.failed` → Marks order as FAILED
4. **Publishes** `payments.requested` → Payment Service
5. **Consumes** `payments.succeeded` → Marks order as COMPLETED
6. **Consumes** `payments.failed` → Marks order as FAILED, publishes `inventory.release`
7. **Publishes** `inventory.release` → Inventory Service (compensation)

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/orders/` | Create a new order |
| `GET` | `/api/orders/` | List all orders |
| `GET` | `/api/orders/{id}/` | Get order details |

### Create Order Example

```bash
curl -X POST http://127.0.0.1:8001/api/orders/ \
  -H "Content-Type: application/json" \
  -H "X-Correlation-ID: 550e8400-e29b-41d4-a716-446655440000" \
  -d '{
    "customer": "3ba8f8e8-3a87-4b32-8194-eab85a81c2d4",
    "items": [
      {
        "product_id": "64da52ff-7c52-447b-bfff-f11d7ef1ab88",
        "quantity": 2,
        "price": "100.00"
      }
    ]
  }'
```

## What Happens When It Reacts

### On `inventory.reserved` (success)
- Updates order status: `PENDING → INVENTORY_RESERVED`
- Publishes `payments.requested` to trigger payment processing

### On `inventory.failed` (failure)
- Updates order status: `PENDING → FAILED`
- No compensation needed (nothing was reserved)

### On `payments.succeeded` (success)
- Updates order status: `INVENTORY_RESERVED → COMPLETED`

### On `payments.failed` (failure)
- Updates order status: `INVENTORY_RESERVED → FAILED`
- Publishes `inventory.release` to compensate (return reserved stock)

## Models

| Model | Purpose |
|-------|---------|
| `Customer` | Customer information |
| `Order` | Order header (status, total, timestamps) |
| `OrderItem` | Line items (product, quantity, price) |
| `ProcessedEvent` | Idempotency tracking |
| `OutboxEvent` | Transactional outbox for Kafka publishing |
| `EventHistory` | Audit ledger for event lifecycle |

## Configuration

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATABASE_URL` | `postgres://...` | PostgreSQL connection |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `SERVICE_NAME` | `order-service` | Service identifier in logs |

## Directory Structure

```
order_service/
├── config/                     # Django settings, URLs, WSGI
├── core/logging/               # JSON logging infrastructure
│   ├── config.py               # Logging configuration
│   ├── context.py              # contextvars for correlation IDs
│   ├── filters.py              # ContextFilter, HealthCheckFilter
│   ├── formatter.py            # JsonFormatter
│   └── middleware.py           # CorrelationMiddleware
├── orders/
│   ├── api/                    # DRF views, serializers, URLs
│   ├── events/
│   │   ├── audit/              # EventHistory ledger
│   │   ├── event_envelope.py   # Message contract
│   │   ├── order_events.py     # Topic constants
│   │   ├── idempotency.py      # ProcessedEvent check/mark
│   │   ├── kafka_consumer.py   # Event handlers
│   │   ├── kafka_publisher.py  # Direct publisher (legacy)
│   │   ├── exceptions.py       # Retryable/NonRetryable
│   │   ├── retry_policy.py     # MAX_RETRIES = 3
│   │   ├── retry_publisher.py  # *.retry producer
│   │   ├── dlq_publisher.py    # *.dlq producer
│   │   ├── failure_handler.py  # Retry/DLQ routing
│   │   └── outbox_service.py   # Outbox CRUD
│   ├── management/commands/
│   │   ├── consume_events.py   # Start Kafka consumer
│   │   └── publish_outbox.py   # Outbox publisher
│   ├── models/                 # Order, OrderItem, etc.
│   └── services/
│       └── order_service.py    # Business logic
├── logs/                       # Structured JSON logs
├── Procfile                    # Process definitions
├── docker-compose.yml          # PostgreSQL container
├── Dockerfile                  # Container image
└── requirements.txt            # Python dependencies
```

## Testing

```bash
python manage.py test
```

Tests cover:
- Order creation and validation
- Status transitions
- Event publishing
- Idempotency checks
- Error handling

## Monitoring

Logs are written to `logs/application.log` in JSON format and aggregated by Grafana Alloy → Loki → Grafana.

Filter logs by correlation ID:
```
{service="order-service"} |= "<correlation-id>"
```