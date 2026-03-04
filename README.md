# SPARQLRace

    $ bundle install
    $ QLEVER=http://example.org/qlever \
    VIRTUOSO=http://example.org/virtuoso \
    TIMEOUT_SEC=5 \
    PORT=4567 \
    BIND=0.0.0.0 \
    bundle exec ruby sparqlrace.rb
