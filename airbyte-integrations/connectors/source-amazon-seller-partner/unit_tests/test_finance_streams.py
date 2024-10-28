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
from source_amazon_seller_partner.streams import ListFinancialEventGroups, ListFinancialEvents, ListTransactions, RestockInventoryReports

list_transactions_data = {
        "transactions": [
            {
                "sellingPartnerMetadata": {
                    "sellingPartnerId": "A1WSDY9MTKXUGZ",
                    "marketplaceId": "AT614YYYNOC1S",
                    "accountType": "Standard Orders"
                },
                "transactionType": "Shipment",
                "transactionId": "lo6cQ55-40s5sU4g_lSwkZV22qz-vCNVtjLLqAQyux4",
                "transactionStatus": None,
                "relatedIdentifiers": [
                    {
                        "relatedIdentifierName": "ORDER_ID",
                        "relatedIdentifierValue": "931-0852306-5138236"
                    },
                    {
                        "relatedIdentifierName": "SHIPMENT_ID",
                        "relatedIdentifierValue": "14178712390725"
                    },
                    {
                        "relatedIdentifierName": "FINANCIAL_EVENT_GROUP_ID",
                        "relatedIdentifierValue": "_fSsCZmGkBME7DCFtbqKLcDX-KB-M-5Pdr1NaefqCks"
                    }
                ],
                "totalAmount": {
                    "currencyAmount": 294.46,
                    "currencyCode": "SEK"
                },
                "description": "Order Payment",
                "postedDate": "2024-01-18T12:45:47Z",
                "marketplaceDetails": {
                    "marketplaceId": "AT614YYYNOC1S",
                    "marketplaceName": "Amazon.se"
                },
                "items": [
                    {
                        "description": "SAMSUNG Galaxy Tab S6 Lite WiFi - 64GB, 4GB, Grigio",
                        "totalAmount": {
                            "currencyAmount": 294.46,
                            "currencyCode": "SEK"
                        },
                        "relatedIdentifiers": [
                            {
                                "itemRelatedIdentifierName": "ORDER_ADJUSTMENT_ITEM_ID",
                                "itemRelatedIdentifierValue": "6226957701205"
                            }
                        ],
                        "breakdowns": [
                            {
                                "breakdownType": "Tax",
                                "breakdownAmount": {
                                    "currencyAmount": 89.8,
                                    "currencyCode": "SEK"
                                },
                                "breakdowns": [
                                    {
                                        "breakdownType": "OurPriceTax",
                                        "breakdownAmount": {
                                            "currencyAmount": 80,
                                            "currencyCode": "SEK"
                                        },
                                        "breakdowns": []
                                    },
                                    {
                                        "breakdownType": "ShippingTax",
                                        "breakdownAmount": {
                                            "currencyAmount": 9.8,
                                            "currencyCode": "SEK"
                                        },
                                        "breakdowns": []
                                    }
                                ]
                            },
                            {
                                "breakdownType": "ProductCharges",
                                "breakdownAmount": {
                                    "currencyAmount": 320,
                                    "currencyCode": "SEK"
                                },
                                "breakdowns": [
                                    {
                                        "breakdownType": "OurPricePrincipal",
                                        "breakdownAmount": {
                                            "currencyAmount": 320,
                                            "currencyCode": "SEK"
                                        },
                                        "breakdowns": []
                                    }
                                ]
                            },
                            {
                                "breakdownType": "Shipping",
                                "breakdownAmount": {
                                    "currencyAmount": 39.2,
                                    "currencyCode": "SEK"
                                },
                                "breakdowns": [
                                    {
                                        "breakdownType": "ShippingPrincipal",
                                        "breakdownAmount": {
                                            "currencyAmount": 39.2,
                                            "currencyCode": "SEK"
                                        },
                                        "breakdowns": []
                                    }
                                ]
                            },
                            {
                                "breakdownType": "AmazonFees",
                                "breakdownAmount": {
                                    "currencyAmount": -154.54,
                                    "currencyCode": "SEK"
                                },
                                "breakdowns": [
                                    {
                                        "breakdownType": "FBAPerUnitFulfillmentFee",
                                        "breakdownAmount": {
                                            "currencyAmount": -45.54,
                                            "currencyCode": "SEK"
                                        },
                                        "breakdowns": [
                                            {
                                                "breakdownType": "Base",
                                                "breakdownAmount": {
                                                    "currencyAmount": -36.43,
                                                    "currencyCode": "SEK"
                                                }
                                            },
                                            {
                                                "breakdownType": "Tax",
                                                "breakdownAmount": {
                                                    "currencyAmount": -9.11,
                                                    "currencyCode": "SEK"
                                                }
                                            }
                                        ]
                                    },
                                    {
                                        "breakdownType": "Commission",
                                        "breakdownAmount": {
                                            "currencyAmount": -60,
                                            "currencyCode": "SEK"
                                        },
                                        "breakdowns": [
                                            {
                                                "breakdownType": "Base",
                                                "breakdownAmount": {
                                                    "currencyAmount": -60,
                                                    "currencyCode": "SEK"
                                                }
                                            }
                                        ]
                                    },
                                    {
                                        "breakdownType": "ShippingChargeback",
                                        "breakdownAmount": {
                                            "currencyAmount": -49,
                                            "currencyCode": "SEK"
                                        },
                                        "breakdowns": [
                                            {
                                                "breakdownType": "Base",
                                                "breakdownAmount": {
                                                    "currencyAmount": -39.2,
                                                    "currencyCode": "SEK"
                                                }
                                            },
                                            {
                                                "breakdownType": "Tax",
                                                "breakdownAmount": {
                                                    "currencyAmount": -9.8,
                                                    "currencyCode": "SEK"
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        ],
                        "contexts": [
                            {
                                "asin": "B085V6LQPB",
                                "quantityShipped": 1,
                                "sku": "PSMM-TEST-SKU-Aug-29_08_03_14-0898",
                                "fulfillmentNetwork": "AFN",
                                "contextType": "ProductContext"
                            }
                        ]
                    }
                ],
                "breakdowns": [
                    {
                        "breakdownType": "Sales",
                        "breakdownAmount": {
                            "currencyAmount": 449,
                            "currencyCode": "SEK"
                        },
                        "breakdowns": [
                            {
                                "breakdownType": "Tax",
                                "breakdownAmount": {
                                    "currencyAmount": 89.8,
                                    "currencyCode": "SEK"
                                },
                                "breakdowns": None
                            },
                            {
                                "breakdownType": "ProductCharges",
                                "breakdownAmount": {
                                    "currencyAmount": 320,
                                    "currencyCode": "SEK"
                                },
                                "breakdowns": None
                            },
                            {
                                "breakdownType": "Shipping",
                                "breakdownAmount": {
                                    "currencyAmount": 39.2,
                                    "currencyCode": "SEK"
                                },
                                "breakdowns": None
                            }
                        ]
                    },
                    {
                        "breakdownType": "Expenses",
                        "breakdownAmount": {
                            "currencyAmount": -154.54,
                            "currencyCode": "SEK"
                        },
                        "breakdowns": [
                            {
                                "breakdownType": "AmazonFees",
                                "breakdownAmount": {
                                    "currencyAmount": -154.54,
                                    "currencyCode": "SEK"
                                },
                                "breakdowns": None
                            }
                        ]
                    }
                ],
                "contexts": None
            }
        ]
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
    def _internal(start_date: str = START_DATE_1, end_date: str = END_DATE_1):
        stream = ListTransactions(
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

def test_list_transactions_stream_path(list_transactions_stream):
    stream = list_transactions_stream()
    assert stream.path() == "finances/v2024-06-19/transactions"

def test_list_transactions_stream_request_params(list_transactions_stream):
    stream = list_transactions_stream()
    params = stream.request_params(
        stream_state={},
        stream_slice={"marketplaceId": "ATVPDKIKX0DER"}
    )
    assert params["postedAfter"] == START_DATE_1
    assert params["postedBefore"] == END_DATE_1
    assert params["marketplaceId"] == "ATVPDKIKX0DER"

def test_list_transactions_stream_parse_response(list_transactions_stream):
    stream = list_transactions_stream()
    response = requests.Response()
    response.json = mock.Mock(return_value=list_transactions_data)
    
    parsed_records = list(stream.parse_response(response))
    assert len(parsed_records) == 1
    assert parsed_records[0]["transactionId"] == "lo6cQ55-40s5sU4g_lSwkZV22qz-vCNVtjLLqAQyux4"
    assert parsed_records[0]["transactionType"] == "Shipment"
    assert parsed_records[0]["totalAmount"]["currencyAmount"] == 294.46
    assert parsed_records[0]["totalAmount"]["currencyCode"] == "SEK"
    assert parsed_records[0]["marketplaceDetails"]["marketplaceName"] == "Amazon.se"

def test_list_transactions_stream_backoff_time(list_transactions_stream):
    stream = list_transactions_stream()
    response = requests.Response()
    response.headers = {"x-amzn-RateLimit-Limit": "1.0"}
    assert stream.backoff_time(response) == 1.0

    response.headers = {}
    assert stream.backoff_time(response) == 60

def test_list_transactions_stream_next_page_token(list_transactions_stream):
    stream = list_transactions_stream()
    response = requests.Response()
    response.json = mock.Mock(return_value={
        "payload": {"NextToken": "next_token_value"}
    })
    assert stream.next_page_token(response) == {"NextToken": "next_token_value"}

    response.json = mock.Mock(return_value={"payload": {}})
    assert stream.next_page_token(response) is None