set -e

mongo <<EOF
db = db.getSiblingDB('frauddb')

db.createUser({
  user: 'fraud_user',
  pwd: 'fraud_pass',
  roles: [{ role: 'readWrite', db: 'frauddb' }],
});
db.createCollection('billing')

EOF
