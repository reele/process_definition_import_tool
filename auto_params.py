from typing import Any


class AutoParams:
    def __init__(self, seqs: dict[str, Any]):
        self.params = seqs

    def next(self, key):
        """从集合中取对象的下一个迭代值
        int : += 1
        list: pop()
        """
        value = self.params[key]
        if isinstance(value, int):
            self.params[key] = value + 1
            return value
        elif isinstance(value, list):
            return value.pop()
        else:
            raise TypeError()

    def get(self, key):
        """从集合中取对象的值"""
        value = self.params[key]
        return value
        # if isinstance(value, int):
        #     return value
        # elif isinstance(value, str):
        #     return value
        # elif isinstance(value, list):
        #     return value[-1]
        # else:
        #     raise TypeError()
    
    def pick(self, params_key, key, key2=None):
        """从集合中取出对象
        dict: pop()
        """
        if key2:
            value = self.params[params_key][key]
            if isinstance(value, dict):
                return value.pop(key2)
            else:
                raise TypeError()
        else:
            value = self.params[params_key]
            if isinstance(value, dict):
                return value.pop(key)
            else:
                raise TypeError()

    def reset(self, str, value):
        self.params[str] = value
