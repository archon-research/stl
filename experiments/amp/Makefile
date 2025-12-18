.PHONY: install start stop query clean

# Install Amp using ampup
install:
	@./scripts/install.sh

# Start PostgreSQL and Amp server
start:
	@./scripts/start.sh

# Start only PostgreSQL (for development)
start-db:
	docker-compose up -d

# Stop all services
stop:
	docker-compose down

# Query Amp server (usage: make query SQL="SELECT 1")
query:
	@./scripts/query.sh "$(SQL)"

# Clean up volumes and data
clean:
	docker-compose down -v

# Show logs
logs:
	docker-compose logs -f

# Check status
status:
	@echo "=== Docker Containers ==="
	docker-compose ps
	@echo ""
	@echo "=== Amp Server ==="
	@curl -s -X POST http://localhost:1603 --data 'SELECT 1' 2>/dev/null && echo " (Amp is running)" || echo " (Amp is not running)"
