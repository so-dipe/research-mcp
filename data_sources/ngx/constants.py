from pathlib import Path
from typing import Literal

BASE_STORAGE = Path(__file__).resolve().parents[2] / "storage"
MANIFESTS_DIR = BASE_STORAGE / "manifests"
RAW_DOCS_DIR = BASE_STORAGE / "raw_docs"
TABLES_DIR = BASE_STORAGE / "tables"
PROCESSED_DIR = BASE_STORAGE / "processed_docs"

NGX_INSTITUTIONS_URL= "https://doclib.ngxgroup.com/REST/api/issuers/companydirectory?$orderby=CompanyName"

NGX_DOCS_URL = "https://doclib.ngxgroup.com/_api/Web/Lists/GetByTitle('XFinancial_News')/items/"

NGX_DOCS_PARAMS = {
    "$select": "URL,Modified,InternationSecIN,Type_of_Submission",
    "$orderby": "Modified desc"
}

SUBMISSION_FILTERS = {
    "Financial Statement": ["Financial Statements", "EarningsForcast"],
    "Corporate Actions": ["Corporate Actions", "Corporate Disclosures"],
    "Director Dealings": ["DirectorDealings", "Director Dealings"]
}

NGX_INSTITUTIONS_LITERAL = Literal[
    '$id', 'InternationSecIN', 'Symbol', 'PrevClose', 'OpenPrice',
    'DaysHigh', 'DaysLow', 'Volume', 'Value', 'MarketCap',
    'SharesOutstanding', 'Dividend', 'Yield', 'Sector', 'SubSector',
    'CompanyName', 'MarketClassification', 'DateListed',
    'DateOfIncorporation', 'Website', 'Logourl', 'StockPricePercChange',
    'StockPriceChange', 'StockPriceCur', 'CompanyProfileSummary',
    'NatureofBusiness', 'CompanyAddress', 'Telephone', 'Fax', 'Email',
    'CompanySecretary', 'Auditor', 'Registrars', 'BoardOfDirectors', 'ID',
    'HIGH52WK_PRICE', 'HIGH52WK_DATETIME', 'LOW52WK_PRICE',
    'LOW52WK_DATETIME', 'Symbol2', 'LS_STD', 'OFFICIAL_OPEN',
    'OFFICIAL_CLOSE', 'LastUploadInfo', 'QuaterID', 'QuaterYear',
    'TotalAssetsUnderManagement', 'imgPath', 'Address', 'Cable', 'PAddress',
    'Telex'
]