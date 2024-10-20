#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from unittest import mock

import pendulum
import pytest
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.utils import AirbyteTracedException
from airbyte_protocol.models import FailureType
from source_amazon_seller_partner.streams import ListFinancialEventGroups, ListFinancialEvents, RestockInventoryReports, ListFinancialTransactions

list_transactions_data = {
    "transactions": [
        {
            "sellingPartnerMetadata": {
                "sellingPartnerId": "A2EXAMPLE",
                "accountType": "Seller",
                "marketplaceId": "ATVPDKIKX0DER"
            },
            "relatedIdentifiers": [
                {
                    "relatedIdentifierName": "ORDER_ID",
                    "relatedIdentifierValue": "123-1234567-1234567"
                }
            ],
            "transactionType": "Shipment",
            "transactionId": "amzn1.transaction.txid.A2EXAMPLE.XXXXXXX",
            "transactionStatus": "Released",
            "description": "Transaction description",
            "postedDate": "2023-01-01T00:00:00Z",
            "totalAmount": {
                "currencyCode": "USD",
                "currencyAmount": 100.00
            },
            "marketplaceDetails": {
                "marketplaceId": "ATVPDKIKX0DER",
                "marketplaceName": "Amazon.com"
            },
            "items": [
                {
                    "description": "Item description",
                    "totalAmount": {
                        "currencyCode": "USD",
                        "currencyAmount": 100.00
                    }
                }
            ]
        }
    ],
    "nextToken": "next_token_value"
}


list_financial_event_groups_data = {
    "payload": {
        "FinancialEventGroupList": [
            {
                "FinancialEventGroupId": "id",
                "ProcessingStatus": "Closed",
                "FundTransferStatus": "Succeeded",
                "OriginalTotal": {"CurrencyCode": "CAD", "CurrencyAmount": 1.0},
                "ConvertedTotal": {"CurrencyCode": "USD", "CurrencyAmount": 2.0},
                "FundTransferDate": "2022-05-14T19:24:35Z",
                "TraceId": "1, 2",
                "AccountTail": "181",
                "BeginningBalance": {"CurrencyCode": "CAD", "CurrencyAmount": 0.0},
                "FinancialEventGroupStart": "2022-04-29T19:15:59Z",
                "FinancialEventGroupEnd": "2022-05-13T19:15:59Z",
            }
        ]
    }
}

list_financial_events_data = {
    "payload": {
        "FinancialEvents": {
            "ShipmentEventList": [
                {
                    "AmazonOrderId": "A_ORDER_ID",
                    "SellerOrderId": "S_ORDER_ID",
                    "MarketplaceName": "Amazon.com",
                    "PostedDate": "2022-05-01T01:32:42Z",
                    "ShipmentItemList": [],
                }
            ],
            "RefundEventList": [
                {
                    "AmazonOrderId": "A_ORDER_ID",
                    "SellerOrderId": "S_ORDER_ID",
                    "MarketplaceName": "Amazon.ca",
                    "PostedDate": "2022-05-01T03:05:36Z",
                    "ShipmentItemAdjustmentList": [],
                }
            ],
            "GuaranteeClaimEventList": [],
            "ChargebackEventList": [],
            "PayWithAmazonEventList": [],
            "ServiceProviderCreditEventList": [],
            "RetrochargeEventList": [],
            "RentalTransactionEventList": [],
            "PerformanceBondRefundEventList": [],
            "ProductAdsPaymentEventList": [],
            "ServiceFeeEventList": [],
            "SellerDealPaymentEventList": [],
            "DebtRecoveryEventList": [],
            "LoanServicingEventList": [],
            "AdjustmentEventList": [
                {
                    "AdjustmentType": "XXXX",
                    "PostedDate": "2022-05-01T15:08:00Z",
                    "AdjustmentAmount": {"CurrencyCode": "USD", "CurrencyAmount": 25.35},
                    "AdjustmentItemList": [],
                }
            ],
            "SAFETReimbursementEventList": [],
            "SellerReviewEnrollmentPaymentEventList": [],
            "FBALiquidationEventList": [],
            "CouponPaymentEventList": [],
            "ImagingServicesFeeEventList": [],
            "TaxWithholdingEventList": [],
            "NetworkComminglingTransactionEventList": [],
            "AffordabilityExpenseEventList": [],
            "AffordabilityExpenseReversalEventList": [],
            "RemovalShipmentAdjustmentEventList": [],
            "RemovalShipmentEventList": [],
        }
    }
}

DATE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

START_DATE_1 = "2022-05-25T00:00:00Z"
END_DATE_1 = "2022-05-26T00:00:00Z"

START_DATE_2 = "2021-01-01T00:00:00Z"
END_DATE_2 = "2022-07-31T00:00:00Z"


@pytest.fixture
def list_financial_event_groups_stream():
    def _internal(start_date: str = START_DATE_1, end_date: str = END_DATE_1):
        stream = ListFinancialEventGroups(
            url_base="https://test.url",
            replication_start_date=start_date,
            replication_end_date=end_date,
            marketplace_id="id",
            authenticator=None,
            period_in_days=0,
            report_options=None,
        )
        return stream

    return _internal


@pytest.fixture
def list_financial_events_stream():
    def _internal(start_date: str = START_DATE_1, end_date: str = END_DATE_1):
        stream = ListFinancialEvents(
            url_base="https://test.url",
            replication_start_date=start_date,
            replication_end_date=end_date,
            marketplace_id="id",
            authenticator=None,
            period_in_days=0,
            report_options=None,
        )
        return stream

    return _internal


def test_finance_stream_next_token(mocker, list_financial_event_groups_stream):
    response = requests.Response()
    token = "aabbccddeeff"
    expected = {"NextToken": token}
    mocker.patch.object(response, "json", return_value={"payload": expected})
    assert expected == list_financial_event_groups_stream().next_page_token(response)

    mocker.patch.object(response, "json", return_value={"payload": {}})
    if list_financial_event_groups_stream().next_page_token(response) is not None:
        assert False


def test_financial_event_groups_stream_request_params(list_financial_event_groups_stream):
    # test 1
    expected_params = {
        "FinancialEventGroupStartedAfter": START_DATE_1,
        "MaxResultsPerPage": 100,
        "FinancialEventGroupStartedBefore": END_DATE_1,
    }
    assert expected_params == list_financial_event_groups_stream().request_params({}, None)

    # test 2
    token = "aabbccddeeff"
    expected_params = {"NextToken": token}
    assert expected_params == list_financial_event_groups_stream().request_params({}, {"NextToken": token})

    # test 3 - for 180 days limit
    expected_params = {
        "FinancialEventGroupStartedAfter": pendulum.parse(END_DATE_2).subtract(days=180).strftime(DATE_TIME_FORMAT),
        "MaxResultsPerPage": 100,
        "FinancialEventGroupStartedBefore": END_DATE_2,
    }
    assert expected_params == list_financial_event_groups_stream(START_DATE_2, END_DATE_2).request_params({}, None)


def test_financial_event_groups_stream_parse_response(mocker, list_financial_event_groups_stream):
    response = requests.Response()
    mocker.patch.object(response, "json", return_value=list_financial_event_groups_data)

    for record in list_financial_event_groups_stream().parse_response(response, {}):
        assert record == list_financial_event_groups_data.get("payload").get("FinancialEventGroupList")[0]


def test_financial_events_stream_request_params(list_financial_events_stream):
    # test 1
    expected_params = {"PostedAfter": START_DATE_1, "MaxResultsPerPage": 100, "PostedBefore": END_DATE_1}
    assert expected_params == list_financial_events_stream().request_params({}, None)

    # test 2
    token = "aabbccddeeff"
    expected_params = {"NextToken": token}
    assert expected_params == list_financial_events_stream().request_params({}, {"NextToken": token})

    # test 3 - for 180 days limit
    expected_params = {
        "PostedAfter": pendulum.parse(END_DATE_2).subtract(days=180).strftime(DATE_TIME_FORMAT),
        "MaxResultsPerPage": 100,
        "PostedBefore": END_DATE_2,
    }
    assert expected_params == list_financial_events_stream(START_DATE_2, END_DATE_2).request_params({}, None)


def test_financial_events_stream_parse_response(mocker, list_financial_events_stream):
    response = requests.Response()
    mocker.patch.object(response, "json", return_value=list_financial_events_data)

    for record in list_financial_events_stream().parse_response(response, {}):
        assert list_financial_events_data.get("payload").get("FinancialEvents").get("ShipmentEventList") == record.get("ShipmentEventList")
        assert list_financial_events_data.get("payload").get("FinancialEvents").get("RefundEventList") == record.get("RefundEventList")
        assert list_financial_events_data.get("payload").get("FinancialEvents").get("AdjustmentEventList") == record.get(
            "AdjustmentEventList"
        )


def test_reports_read_records_raise_on_backoff(mocker, requests_mock, caplog):
    mocker.patch("time.sleep", lambda x: None)
    requests_mock.post(
        "https://test.url/reports/2021-06-30/reports",
        status_code=429,
        json={
            "errors": [
                {
                    "code": "QuotaExceeded",
                    "message": "You exceeded your quota for the requested resource.",
                    "details": ""
                }
            ]
        },
    )

    stream = RestockInventoryReports(
        stream_name="GET_RESTOCK_INVENTORY_RECOMMENDATIONS_REPORT",
        url_base="https://test.url",
        replication_start_date=START_DATE_1,
        replication_end_date=END_DATE_1,
        marketplace_id="id",
        authenticator=None,
        period_in_days=0,
        report_options=None,
    )
    with pytest.raises(AirbyteTracedException) as exception:
        list(stream.read_records(sync_mode=SyncMode.full_refresh))

    assert exception.value.failure_type == FailureType.transient_error


@pytest.mark.parametrize(
    ("response_headers", "expected_backoff_time"),
    (({"x-amzn-RateLimit-Limit": "2"}, 0.5), ({}, 60)),
)
def test_financial_events_stream_backoff_time(list_financial_events_stream, response_headers, expected_backoff_time):
    stream = list_financial_events_stream()
    response_mock = mock.MagicMock()
    response_mock.headers = response_headers
    assert stream.backoff_time(response_mock) == expected_backoff_time




@pytest.fixture
def list_transactions_stream():
    def _internal(start_date: str = "2023-01-01T00:00:00Z", end_date: str = "2023-01-31T23:59:59Z"):
        return ListFinancialTransactions(
            url_base="https://test.url",
            replication_start_date=start_date,
            replication_end_date=end_date,
            marketplace_id="ATVPDKIKX0DER",
            authenticator=None,
            period_in_days=30,
            report_options=None,
        )
    return _internal

def test_list_transactions_stream_path(list_transactions_stream):
    stream = list_transactions_stream()
    assert stream.path() == "finances/2024-06-19/transactions"

def test_list_transactions_stream_primary_key(list_transactions_stream):
    stream = list_transactions_stream()
    assert stream.primary_key == "transactionId"

def test_list_transactions_stream_cursor_field(list_transactions_stream):
    stream = list_transactions_stream()
    assert stream.cursor_field == "postedDate"

def test_list_transactions_stream_request_params(list_transactions_stream):
    stream = list_transactions_stream()
    params = stream.request_params({})
    assert "postedAfter" in params
    assert "postedBefore" in params
    assert "MaxResultsPerPage" in params
    assert "marketplaceId" in params
    assert params["marketplaceId"] == "ATVPDKIKX0DER"

def test_list_transactions_stream_parse_response(list_transactions_stream, mocker):
    stream = list_transactions_stream()
    response = requests.Response()
    mocker.patch.object(response, "json", return_value=list_transactions_data)

    parsed_records = list(stream.parse_response(response))
    assert len(parsed_records) == 1
    assert parsed_records[0]["transactionId"] == "amzn1.transaction.txid.A2EXAMPLE.XXXXXXX"
    assert parsed_records[0]["transactionType"] == "Shipment"
    assert parsed_records[0]["postedDate"] == "2023-01-01T00:00:00Z"

def test_list_transactions_stream_next_page_token(list_transactions_stream, mocker):
    stream = list_transactions_stream()
    response = requests.Response()
    mocker.patch.object(response, "json", return_value=list_transactions_data)

    next_page_token = stream.next_page_token(response)
    assert next_page_token == {"nextToken": "next_token_value"}

def test_list_transactions_stream_request_params_with_next_page_token(list_transactions_stream):
    stream = list_transactions_stream()
    next_page_token = {"nextToken": "next_token_value"}
    params = stream.request_params({}, next_page_token)
    assert params == {"nextToken": "next_token_value"}

@pytest.mark.parametrize(
    ("response_headers", "expected_backoff_time"),
    (({"x-amzn-RateLimit-Limit": "2"}, 0.5), ({}, 60)),
)
def test_list_transactions_stream_backoff_time(list_transactions_stream, mocker, response_headers, expected_backoff_time):
    stream = list_transactions_stream()
    response_mock = mocker.MagicMock()
    response_mock.headers = response_headers
    assert stream.backoff_time(response_mock) == expected_backoff_time

def test_list_transactions_stream_read_records(list_transactions_stream, mocker):
    stream = list_transactions_stream()
    mocker.patch.object(stream, "read_records", return_value=iter([{"transactionId": "test_id"}]))
    records = list(stream.read_records(sync_mode=SyncMode.full_refresh))
    assert len(records) == 1
    assert records[0]["transactionId"] == "test_id"