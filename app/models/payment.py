from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, constr

class BasePayment(BaseModel):
    payment_id: str = Field(..., description="Unique payment identifier")
    amount: float = Field(..., gt=0)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    status: str = "RECEIVED"
    metadata: Dict[str, Any] = Field(default_factory=dict)

class FiatPayment(BasePayment):
    currency: str = Field(..., min_length=3, max_length=3)

class SwiftPayment(FiatPayment):
    type: str = "SWIFT"
    swift_code: str
    sender_bic: str
    receiver_bic: str
    correspondent_bank: Optional[str] = None
    message_type: Optional[str] = "MT103"
    settlement_method: Optional[str] = "COVR"

class ACHPayment(FiatPayment):
    type: str = "ACH"
    routing_number: str
    account_number: str
    sec_code: str
    addenda_records: Optional[List[str]] = None
    batch_number: Optional[int] = None
    company_entry_description: Optional[str] = None

class WirePayment(FiatPayment):
    type: str = "WIRE"
    routing_number: str
    account_number: str
    bank_name: str
    intermediary_bank: Optional[str] = None
    beneficiary_name: Optional[str] = None
    reference_number: Optional[str] = None

class SEPAPayment(FiatPayment):
    type: str = "SEPA"
    iban: str
    bic: str
    sepa_type: str  # 'SCT' or 'SDD'
    mandate_reference: Optional[str] = None
    creditor_id: Optional[str] = None
    batch_booking: Optional[bool] = False

class RTPPayment(FiatPayment):
    type: str = "RTP"
    clearing_system: str  # 'TCH' or 'FED'
    priority: str = "HIGH"
    purpose_code: Optional[str] = None
    settlement_method: str = "REAL_TIME"

class CryptoPayment(BasePayment):
    type: str = "CRYPTO"
    currency: str = Field(..., min_length=3, max_length=10)  # Allow longer crypto symbols
    network: str  # 'ETH', 'BTC', 'USDC'
    wallet_address: str
    gas_fee: Optional[float] = None
    confirmation_blocks: Optional[int] = None
