from typing import Any, Dict


class Serializable(object):
    def to_dict(self) -> Dict[str, Any]:
        col_map = self.__mapper__.c
        return {col_map[name].name: getattr(self, name) for name in col_map.keys()}
