FROM cassandra:4

# WORKDIR /var/lib/cassandra

# COPY ../script.sh /

# # Override ENTRYPOINT, keep CMD
# ENTRYPOINT ["/script.sh"]
# CMD ["cassandra", "-f"]

# Fix UTF-8 accents in init scripts
ENV LANG C.UTF-8

# Here, you can add any *.sh or *.cql scripts inside /docker-entrypoint-initdb.d
#  *.sh files will be executed BEFORE launching cassandra
#  *.cql files will be executed with cqlsh -f AFTER cassandra started
# Files are executed in name order (ls * | sort)
COPY *.cql /docker-entrypoint-initdb.d/

# this is the script that will patch the already existing entrypoint from cassandra image
COPY entrypoint.sh /

# Override ENTRYPOINT, keep CMD
ENTRYPOINT ["/entrypoint.sh"]
CMD ["cassandra", "-f", "-R"]

