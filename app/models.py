import uuid
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model


class NewSubs(Model):
    id= columns.UUID(primary_key=True, default=uuid.uuid4)
    date= columns.DateTime(custom_index=True)
    number= columns.Integer()
    normalUser= columns.Integer(db_field='normaluser')
    monthly= columns.Integer()
    year= columns.Integer()
    read= columns.Boolean(default=False)

class Tram(Model):
    id= columns.UUID(primary_key=True, default=uuid.uuid4)
    users= columns.Integer()
    station1= columns.Integer()
    station2= columns.Integer()
    station3= columns.Integer()

class FrequentedTram(Model):
    id= columns.UUID(primary_key=True, default=uuid.uuid4)
    day= columns.Text()
    intervalStart= columns.Integer(db_field='intervalstart')
    intervalStop= columns.Integer(db_field='intervalstop')
    tramA= columns.UUID(db_field='trama')
    tramB= columns.UUID(db_field='tramb')
    tramC= columns.UUID(db_field='tramc')

class Trajet(Model):
    id= columns.UUID(primary_key=True, default=uuid.uuid4)
    day= columns.DateTime(custom_index=True)
    intervalStart= columns.Integer(db_field='intervalstart')
    intervalStop= columns.Integer(db_field='intervalstop')
    tram= columns.Text()
    users= columns.Integer()
    read= columns.Boolean(default=False)

class Station(Model):
    id= columns.UUID(primary_key=True, default=uuid.uuid4)
    day= columns.DateTime(custom_index=True)
    intervalStart= columns.Integer(db_field='intervalstart')
    intervalStop= columns.Integer(db_field='intervalstop')
    name= columns.Text()
    users= columns.Integer()
    read= columns.Boolean(default=False)

