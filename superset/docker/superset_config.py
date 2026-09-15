cat >> .gitignore <<'EOF'

# Superset local configuration
superset/docker/.env
superset/docker/superset_home/

# DuckDB
data/analytics/*.duckdb
EOF
