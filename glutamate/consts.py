from typing import Literal


E621_STATIC_URL = "https://static1.e621.net/data"
CATEGORY_TO_NUMBERS = {
    'general': 0,
    'artist': 1,
    'rating': 2,
    'copyright': 3,
    'character': 4,
    'species': 5,
    'invalid': 6,
    'meta': 7,
    'lore': 8
}
NUMBERS_TO_CATEGORY = {
    v:k for k, v in CATEGORY_TO_NUMBERS.items()
}
DEFAULT_CATEGORIES_ORDER: list[Literal['character', 'copyright', 'lore', 'species', 'artist', 'rating', 'general', 'invalid', 'meta']] = [
    'character', 
    'copyright', 
    'lore', 
    'species', 
    'artist', 
    'rating', 
    'general', 
    'invalid', 
    'meta'
]
