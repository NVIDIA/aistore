from aistore.sdk.const import PROVIDER_AIS, PROVIDER_AMAZON, PROVIDER_GOOGLE

BOLD = "\033[1m"
UNDERLINE = "\033[4m"
END = "\033[0m"

PROVIDERS = {
    "ais": PROVIDER_AIS,
    "aws": PROVIDER_AMAZON,
    "gcp": PROVIDER_GOOGLE,
    "gs": PROVIDER_GOOGLE,
    "s3": PROVIDER_AMAZON,
}

BOOLEAN_VALUES = {"false": False, "f": False, "true": True, "t": True}
