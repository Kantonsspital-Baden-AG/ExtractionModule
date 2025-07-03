from typing import Generator


class ExcelIdentifierGenerator:
    def __init__(self) -> None:
        self.current = 0

    def __iter__(self) -> Generator[str, None, None]:
        return self

    def __next__(self) -> str:
        identifier = self._number_to_identifier(self.current)
        self.current += 1
        return identifier

    def generate_next(self) -> str:
        return next(self)

    @staticmethod
    def _number_to_identifier(number) -> str:
        letters = []
        while number >= 0:
            letters.append(chr(number % 26 + ord("A")))
            number = number // 26 - 1
        return "".join(reversed(letters))


# Usage
# id_gen = ExcelIdentifierGenerator()
#
# for i, id in enumerate(id_gen):
#     print(id)
#
# Output:
# A, B, C, ..., Z, AA, AB, AC, ..., AZ, BA, BB, BC, ..., ZZ, AAA, AAB, AAC, ..., ZZZ, ...
