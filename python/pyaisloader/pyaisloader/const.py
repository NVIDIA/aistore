from aistore.sdk.provider import Provider

BOLD = "\033[1m"
UNDERLINE = "\033[4m"
END = "\033[0m"

PROVIDERS = {
    "ais": Provider.AIS,
    "aws": Provider.AMAZON,
    "gcp": Provider.GOOGLE,
    "gs": Provider.GOOGLE,
    "s3": Provider.AMAZON,
}

BOOLEAN_VALUES = {"false": False, "f": False, "true": True, "t": True}
