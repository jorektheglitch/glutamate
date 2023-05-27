from typing import Literal


E621_STATIC_URL = "https://static1.e621.net/data"

POST_COLUMNS = {
    'approver_id',    'change_seq',   'comment_count',    'created_at',       'description',
    'down_score',     'duration',     'fav_count',        'file_ext',         'file_size',
    'id',             'image_height', 'image_width',      'is_deleted',       'is_flagged',
    'is_note_locked', 'is_pending',   'is_rating_locked', 'is_status_locked', 'locked_tags',
    'md5',            'parent_id',    'rating',           'score',            'source',
    'tag_string',     'up_score',     'updated_at',       'uploader_id',
}
TAG_COLUMNS = {'id', 'name', 'category', 'post_count'}

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
