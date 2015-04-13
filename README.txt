requirements:
    python3/redis/postgres/rabbitmq


db init:
    SQL="SELECT COUNT(1) FROM pg_catalog.pg_database WHERE datname = 'pyramid';"
    IS_DATABASE_CREATED=$(sudo -u postgres psql -t -c "$SQL")

    if [ $IS_DATABASE_CREATED -ne 1 ]; then
       cat <<PGSCRIPT | sudo -u postgres psql
    CREATE DATABASE su WITH ENCODING = 'utf8' TEMPLATE template0;
    CREATE DATABASE su_test WITH ENCODING = 'utf8' TEMPLATE template0;
    CREATE USER su_user WITH PASSWORD 'asdf';
    PGSCRIPT
    fi


setup:
    python3 bootstrap.py
    ./bin/buildout


test:
    ./bin/python setup.py test