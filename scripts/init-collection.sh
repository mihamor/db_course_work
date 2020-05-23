#!/usr/bin/env bash
set -e

mongo <<EOF
use $DB
db.createUser({
  user:  '$USER',
  pwd: '$PSW',
  roles: [{
    role: 'readWrite',
    db: '$DB'
  }]
})
db.cwdb.createIndex({ date:"hashed"})
sh.enableSharding('$DB')
sh.shardCollection( "$DB.cwdb", { date:"hashed" })
EOF
