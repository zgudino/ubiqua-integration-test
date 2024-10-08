import locale
import logging
import os
import threading
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from datetime import datetime

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, BulkWriteError

logging.basicConfig(format='%(asctime)s [%(levelname)s] - %(message)s', level=logging.INFO)
load_dotenv()


@dataclass
class OrderItem:
    uid: str
    quantity: int
    price: float
    name: str
    brand: str


@dataclass
class Order:
    uid: str
    date_of_order: datetime
    client_uid: str
    client_name: str
    client_address: str
    latitude: float
    longitude: float
    status: str
    subtotal: float
    taxes: float
    total: float
    is_promotion_day: bool
    most_popular_brand: str
    order_items: tuple[OrderItem, ...]


@contextmanager
def setlocale(code: str):
    with threading.Lock():
        saved = locale.setlocale(locale.LC_ALL)

        try:
            yield locale.setlocale(locale.LC_ALL, code)
        finally:
            locale.setlocale(locale.LC_ALL, saved)


def weekday_short_name(date: datetime, locale_code="en_US.UTF-8"):
    """
    Returns the short name of the weekday for the given date in the given locale.
    :param date: The date to get the weekday for.
    :param locale_code: The locale code to use. Default: "en_US.UTF-8".
    :return: The short name of the weekday.
    """
    with setlocale(locale_code):
        return date.strftime("%a").upper()


if __name__ == "__main__":
    postgresql_connection_string = os.getenv("POSTGRESQL_CONNECTION_STRING", "")
    mongodb_connection_string = os.getenv("MONGODB_CONNECTION_STRING", "")

    with psycopg.connect(postgresql_connection_string) as connection:
        with connection.cursor(name="orders_cursor", row_factory=dict_row) as cursor:
            order_query = cursor.execute("SELECT * FROM orders")
            orders = []

            for order in order_query:
                client = cursor.execute("SELECT * FROM clients WHERE uid = %s", [order["client_uid"]]).fetchone()
                order_details = cursor.execute("SELECT * FROM order_items WHERE order_uid = %s",
                                               [order["uid"]]
                                               ).fetchall()
                order_items = []
                order_subtotal_without_tax = 0
                order_tax_amount = 0
                order_total = 0

                for order_detail in order_details:
                    product = cursor.execute("SELECT * FROM products WHERE uid = %s",
                                             [order_detail["product_uid"]]
                                             ).fetchone()
                    order_subtotal_without_tax += order_detail["quantity"] * float(product["unit_price"])
                    order_tax_amount = order_subtotal_without_tax * float(product["tax_rate"])
                    order_total = order_subtotal_without_tax + order_tax_amount

                    order_items.append(
                        OrderItem(
                            uid=product["uid"],
                            quantity=order_detail["quantity"],
                            price=float(product["unit_price"]),
                            name=product["name"],
                            brand=product["brand"]
                        )
                    )

                is_promotion_day = weekday_short_name(order["date_of_order"]) == client["promotion_day"]
                most_popular_brand = max(order_items, key=lambda oi: oi.quantity).brand

                orders.append(
                    Order(
                        uid=order["uid"],
                        date_of_order=order["date_of_order"],
                        client_uid=client["uid"],
                        client_name=client["name"],
                        client_address=client["address"],
                        latitude=float(order["latitude"]),
                        longitude=float(order["longitude"]),
                        status=order["status"],
                        subtotal=order_subtotal_without_tax,
                        taxes=order_tax_amount,
                        total=order_total,
                        is_promotion_day=is_promotion_day,
                        most_popular_brand=most_popular_brand,
                        order_items=tuple(order_items)
                    )
                )

    client = MongoClient(mongodb_connection_string)
    database = client.get_database("dcnhum24eom32t")
    orders_collection = database.get_collection("orders")
    # Transforms a list of Order objects into a list of dictionaries
    orders_documents = list(map(lambda o: asdict(o), orders))

    logging.info({"count": len(orders_documents), "documents": orders_documents})

    try:
        orders_collection.create_index("uid", unique=True)
        orders_collection.insert_many(orders_documents, ordered=False)
    except DuplicateKeyError as error:
        # Unique index on the 'uid' field already exists
        logging.debug(error.details)
    except BulkWriteError as error:
        # Gracefully ignore 'Collection.insert_many(...)' error due to duplicate keys
        logging.debug(error.details)
