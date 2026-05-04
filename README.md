# ConfigHub — Serveur de configuration dynamique pour microservices

**Projet 11 — Programmation Système avec Rust (GL4)**

## Architecture

```
confighub/
├── src/
│   ├── lib.rs           # Exports publics
│   ├── main.rs          # Serveur principal (Parties 3 + 4 concurrentes)
│   ├── config_store.rs  # Partie 1 : Store avec versioning
│   ├── persistence.rs   # Partie 2 : TOML + env vars + ${références}
│   ├── subscription.rs  # Partie 3 : Serveur TCP de souscriptions
│   ├── http_api.rs      # Partie 4 : API REST avec axum
│   └── client.rs        # Partie 5 : Client Rust avec cache local
├── config_data/
│   ├── db.toml          # Namespace "db"
│   └── app.toml         # Namespace "app"
└── Cargo.toml
```

## Démarrage rapide

```bash
# Compiler et lancer
cargo run --bin confighub-server

# Lancer les tests unitaires
cargo test

# Lancer les tests de charge (serveur doit tourner)
cargo test --release load_test -- --ignored
```

## Partie 1 — Store de configuration avec versioning

Le `ConfigStore` maintient les paires `(namespace, key) → ConfigValue`.
Chaque modification incrémente un compteur de version global.

```rust
let mut store = ConfigStore::new();
store.set("db", "host", "localhost".to_string()); // → version 1
store.set("db", "port", "5432".to_string());       // → version 2

// Récupérer les changements depuis la version 0
let changes = store.changes_since(0);
```

## Partie 2 — Persistance

Chargement initial depuis les fichiers TOML et variables d'environnement :

```bash
# Priorité aux variables d'env sur les fichiers TOML
export CONFIGHUB_DB_HOST=prod-db.example.com
cargo run --bin confighub-server
```

Résolution de références entre clés :
```toml
# config_data/app.toml
[values]
url = "postgres://${db.host}:${db.port}/${db.name}"
```

## Partie 3 — Serveur de souscription TCP (port 7878)

Protocol textuel simple :
```bash
# Connexion avec netcat
nc localhost 7878

# Commandes disponibles
SUBSCRIBE db.host        # souscrit à une clé
SUBSCRIBE db.*           # souscrit à tout un namespace
UNSUBSCRIBE db.host
PING                     # → PONG

# Réponses du serveur
UPDATE db host localhost 3   # namespace key value version
OK SUBSCRIBED db.host
ERROR <message>
```

## Partie 4 — API HTTP REST (port 3000)

```bash
# Lire une valeur
curl http://localhost:3000/config/db/host

# Écrire une valeur
curl -X PUT http://localhost:3000/config/db/host \
     -H "Content-Type: application/json" \
     -d '{"value": "prod-db.example.com"}'

# Supprimer
curl -X DELETE http://localhost:3000/config/db/host

# Export d'un namespace complet
curl http://localhost:3000/config/db

# État du serveur
curl http://localhost:3000/health
```

## Partie 5 — Client Rust

```rust
use confighub::ConfigClient;

let client = ConfigClient::connect(
    "127.0.0.1:7878",   // serveur TCP
    "http://127.0.0.1:3000" // API HTTP
).await?;

// S'abonner à un namespace
client.subscribe("db.*").await?;

// Lire (cache local + fallback HTTP)
let host = client.get("db", "host").await;

// Écrire via HTTP (déclenche un UPDATE vers tous les abonnés)
let version = client.set("db", "host", "new-host").await?;

// Observer une clé en temps réel
tokio::spawn(async move {
    client.watch("db", "host", |new_val| {
        println!("db.host changé → {}", new_val);
    }).await;
});
```

## Tests

```bash
cargo test                          # tous les tests unitaires
cargo test config_store             # tests Partie 1
cargo test persistence              # tests Partie 2
cargo test subscription             # tests Partie 3
cargo test http_api                 # tests Partie 4
```
