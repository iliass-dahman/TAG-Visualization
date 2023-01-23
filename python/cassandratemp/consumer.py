


from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider



# function which checks whether it's a new client or not
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(["2.tcp.eu.ngrok.io"], port="10382",auth_provider=auth_provider)
session = cluster.connect("test")
print(session)