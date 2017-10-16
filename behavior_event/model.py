from abc import ABC, abstractmethod
import datetime
from enum import Enum

from peewee import Model, CharField, TextField, IntegerField, DecimalField, DateTimeField, DateField

from behavior_event.database import get_db_connection, event_db_conf


DT = datetime.datetime
event_db = get_db_connection(**event_db_conf)



class BaseModel(Model, ABC):
    """A base model that will use the "event" database"""

    updated = NotImplemented  # type: str

    # override save method to make the updated timestamp dirty before saving
    def save(self, *args, **kwargs):
        self.updated = DT.now()
        return super(BaseModel, self).save(*args, **kwargs)

    class Meta:
        database = event_db
        schema = "behavior"
        only_save_dirty = True



class EnumField(Enum, CharField):
    # no declared ENUM values in base class

    # override the methods to convert from DB values to Python Values
    def db_value(self, value):
        """Convert the python value for storage in the database."""
        return value if value is None else coerce_to_unicode(value)

    @classmethod
    def python_value(cls, value):
        """Convert the database value to a pythonic value."""
        return value if value is None else cls[coerce_to_unicode(value)]



class ClientTypeField(EnumField):
    FINANCIAL_INSTITUTION = 10
    INBOUND_DATA_PARTNER = 20
    OUTBOUND_BEHAVIOR_POINT_ENGINE = 30
    DATA_WAREHOUSE_STAGING = 40



class StatusField(EnumField):
    PENDING = 10
    ACTIVE = 20
    DISABLED = 30



class Client(BaseModel):
    id = IntegerField(primary_key=True, db_column="client_id", sequence="client_id_seq")
    title = CharField(null=False, unique=True)
    type = ClientTypeField(null=False)
    status = StatusField(null=False, default="PENDING")
    inserted = DateTimeField(default=DT.now, null=False)
    updated = DateTimeField(default=DT.now, null=False)

    class Meta:
        db_table = "clients"



class EventFrequencyField(EnumField):
    EVERY_MINUTE = 5
    HOURLY = 10
    DAILY = 20
    WEEKLY = 30
    MONTHLY = 40
    QUARTERLY = 50
    YEARLY = 60



class BehaviorTypeField(EnumField):
    MINIMUM_ACCOUNT_BALANCE = 10
    MAINTAIN_ACCOUNT_BALANCE = 10
    ADOPT_E_STATEMENT = 30
    OPEN_SAFE_DEPOSIT_BOX = 40



class EventType(BaseModel):
    id = IntegerField(primary_key=True, db_column="event_type_id", sequence="event_type_id_seq")
    title = CharField(null=False, unique=True)
    description = CharField()
    behavior_type = BehaviorTypeField(null=False)
    frequency = EventFrequencyField(null=False)
    stubbed_extract_script = TextField(null=False)
    inserted = DateTimeField(default=DT.now, null=False)
    updated = DateTimeField(default=DT.now, null=False)

    class Meta:
        db_table = "event_types"



class AccountTypeField(EnumField):
    ALL = 1
    INTERNAL = 10
    CHECKING = 20
    CREDIT_CARD = 30
    SAVING = 40
    MONEY_MARKET = 50
    MORTGAGE = 60
    LOAN = 70
    BUSINESS_LOAN = 70
    PERSONAL_LOAN = 70



class ClientConfig(BaseModel):
    id = IntegerField(primary_key=True, db_column="client_config_id", sequence="client_config_id_seq")
    client_id = ForeignKeyField(Client, related_name="configurations", to_field="client_id", index=True)
    event_type_id = ForeignKeyField(EventType, related_name="event_types", to_field="event_type_id", index=True)
    effective_date = DateField(null=False)
    expiration_date = DateField(null=True)
    status = StatusField(null=False, default="PENDING")
    account_type = AccountTypeField(null=False)
    product_code = CharField(null=True)
    # balance_range = DecimalField(max_digits=15, decimal_places=4, auto_round=True, null=True)
    balance_floor = DecimalField(max_digits=15, decimal_places=4, auto_round=True, null=True)
    inserted = DateTimeField(default=DT.now, null=False)
    updated = DateTimeField(default=DT.now, null=False)

    class Meta:
        db_table = "client_configs"



class DestinationSystemField(EnumField):
    DATA_WAREHOUSE_STAGING = 10
    BEHAVIOR_POINT_ENGINE = 20



class Destination(BaseModel):
    id = IntegerField(primary_key=True, db_column="destination_id", sequence="destination_id_seq")
    destination_system = DestinationSystemField(null=False)
    inserted = DateTimeField(default=DT.now, null=False)
    updated = DateTimeField(default=DT.now, null=False)

    class Meta:
        db_table = "destinations"



class DestinationConfig(BaseModel):
    id = IntegerField(primary_key=True, db_column="destination_config_id", sequence="destination_config_id_seq")
    destination_id = ForeignKeyField(Destination, related_name="configurations", to_field="destination_id", index=True)
    key = CharField(null=True)
    value = CharField(null=True)
    inserted = DateTimeField(default=DT.now, null=False)
    updated = DateTimeField(default=DT.now, null=False)

    class Meta:
        db_table = "destination_configs"



class BatchStatusField(EnumField):
    NEW = 10
    PENDING = 10
    STARTED = 20
    DELIVERED = 30
    SUCCESS = 40
    PARTIAL_FAILURE = 50
    FAILURE = 60



class EventBatch(BaseModel):
    id = IntegerField(primary_key=True, db_column="event_batch_id", sequence="event_batch_id_seq")
    client_config_id = ForeignKeyField(ClientConfig, related_name="batches", to_field="client_config_id", index=True)
    status = BatchStatusField(null=False)
    delivery_system_event_batch_id = IntegerField(null=True)
    inserted = DateTimeField(default=DT.now, null=False)
    updated = DateTimeField(default=DT.now, null=False)

    class Meta:
        db_table = "batches"



class EventBatchDelivery(BaseModel):
    id = IntegerField(primary_key=True, db_column="event_batch_delivery_id", sequence="event_batch_delivery_id_seq")
    event_batch_id = ForeignKeyField(EventBatch, related_name="deliveries", to_field="event_batch_id", index=True)
    status = BatchStatusField(null=False)
    status_message = TextField()
    inserted = DateTimeField(default=DT.now, null=False)
    updated = DateTimeField(default=DT.now, null=False)

    class Meta:
        db_table = "event_batch_deliveries"



class Event(BaseModel):
    id = IntegerField(primary_key=True, db_column="event_id", sequence="event_id_seq")
    event_batch_id = ForeignKeyField(EventBatch, related_name="events", to_field="event_batch_id", index=True)
    quantity = DecimalField(max_digits=12, decimal_places=4, auto_round=True, null=False)
    event_timestamp = DateTimeField(null=False)
    inserted = DateTimeField(default=DT.now, null=False)
    updated = DateTimeField(default=DT.now, null=False)

    class Meta:
        db_table = "events"



class EventBatchDeliveryErrorEvent(BaseModel):
    id = IntegerField(primary_key=True, db_column="event_batch_delivery_error_event_id", sequence="event_batch_delivery_error_event_id_seq")
    event_batch_delivery_id = ForeignKeyField(EventBatchDelivery, related_name="errors", to_field="event_batch_delivery_id", index=True)
    event_id = ForeignKeyField(Event, related_name="events", to_field="event_id", index=True)
    inserted = DateTimeField(default=DT.now, null=False)
    updated = DateTimeField(default=DT.now, null=False)

    class Meta:
        db_table = "event_batch_delivery_error_events"

