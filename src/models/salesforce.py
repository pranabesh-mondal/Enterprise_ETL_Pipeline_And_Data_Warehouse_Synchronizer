"""Pydantic models for Salesforce REST API resources (Day 1-2, Week 1)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import AliasChoices, Field, field_validator

from .base import BaseSchema


def parse_iso_datetime(value: Any) -> Any:
    """Salesforce returns ISO-8601 strings - let Pydantic parse them."""
    return value


class SalesforceBase(BaseSchema):
    """Salesforce objects use PascalCase field names; normalize to snake_case."""

    id: str = Field(validation_alias=AliasChoices("Id", "id"))
    created_date: datetime | None = Field(
        default=None,
        validation_alias=AliasChoices("CreatedDate", "created_date"),
    )
    last_modified_date: datetime | None = Field(
        default=None,
        validation_alias=AliasChoices("LastModifiedDate", "last_modified_date"),
    )


class SalesforceAccount(SalesforceBase):
    name: str = Field(validation_alias=AliasChoices("Name", "name"))
    industry: str | None = Field(
        default=None, validation_alias=AliasChoices("Industry", "industry")
    )
    website: str | None = Field(
        default=None, validation_alias=AliasChoices("Website", "website")
    )


class SalesforceOpportunity(SalesforceBase):
    name: str = Field(validation_alias=AliasChoices("Name", "name"))
    amount: float | None = Field(
        default=None, validation_alias=AliasChoices("Amount", "amount")
    )
    stage_name: str = Field(validation_alias=AliasChoices("StageName", "stage_name"))
    close_date: str | None = Field(
        default=None, validation_alias=AliasChoices("CloseDate", "close_date")
    )


# resource name -> (validating model, SOQL query)
SALESFORCE_RESOURCE_QUERIES: dict[str, str] = {
    "accounts": "SELECT Id, Name, Industry, Website, CreatedDate, LastModifiedDate FROM Account",
    "opportunities": "SELECT Id, Name, Amount, StageName, CloseDate, CreatedDate, LastModifiedDate FROM Opportunity",
}

SALESFORCE_RESOURCE_MODELS: dict[str, type[BaseSchema]] = {
    "accounts": SalesforceAccount,
    "opportunities": SalesforceOpportunity,
}
