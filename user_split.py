import hashlib


def get_group(user, salt: str = "a64a4ae0632e7849234c863b44c63ee568c340057bc2c52294cf8b6b04779028", group_count: int = 2) -> int:
    value_str = str(user) + salt
    value_num = int(hashlib.md5(value_str.encode()).hexdigest(), 16)
    group = value_num % group_count
    return "control" if group == 0 else "test"
