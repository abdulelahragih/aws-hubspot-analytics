import os
import logging

from deals import deals_handler
from activities import activities_handler
from deals_raw import raw_deals_handler
from owners_dim import owners_dim_handler
from companies import companies_handler
from contacts import contacts_handler

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)


def handler(event, context):
    """Dispatcher entrypoint. Selects task based on TASK env var.

    This allows multiple Lambda functions to reuse the same image
    and choose behavior via environment variable.
    """
    task = os.environ.get("TASK", "deals").lower()
    if task == "activities":
        return activities_handler(event, context)
    if task == "deals":
        return deals_handler(event, context)
    if task == "deals_raw":
        return raw_deals_handler(event, context)
    if task == "owners_dim":
        return owners_dim_handler(event, context)
    if task == "companies":
        return companies_handler(event, context)
    if task == "contacts":
        return contacts_handler(event, context)
    LOG.warning("Unknown TASK '%s' — defaulting to deals", task)
    return deals_handler(event, context)
