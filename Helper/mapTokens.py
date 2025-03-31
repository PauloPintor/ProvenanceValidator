import logging
import re


class mapTokens:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def replace_words_with_fixed_number(self, s, number=1):
        """
        Replace all words matching the pattern (one or more uppercase letters followed by one or more digits)
        with the same fixed number.

        Args:
            s (str): The input string.
            number (int): The number to replace all matches with (default: 1).

        Returns:
            str: The transformed string.
        """
        # Regex pattern to match words (one or more uppercase letters followed by one or more digits)
        pattern = re.compile(r"\b[A-Za-z]+\d+\b")

        # Replace all matches with the same number
        return pattern.sub(str(number), s)

    def replace_words_with_tokens(self, s):
        """
        Replace words that match the pattern (one or more uppercase letters followed by one or more digits)
        with tokens (x1, x2, x3, ...). If a word appears more than once, it is replaced with the same token.

        Args:
            s (str): The input string.

        Returns:
            tuple: A tuple containing:
                - The transformed string.
                - A dictionary mapping original words to tokens.
        """
        mapping = {}
        counter = [1]  # Using a list for a mutable integer in the nested function

        # Define the replacement function
        def repl(match):
            word = match.group(0)
            if word not in mapping:
                mapping[word] = f"x{counter[0]}"
                counter[0] += 1
            return mapping[word]

        # Regex pattern to match words (one or more uppercase letters followed by one or more digits)
        pattern = re.compile(r"\b[A-Z]+\d+\b")

        # Replace the words using re.sub with the repl function
        result = pattern.sub(repl, s)
        return result, mapping

    def replace_tokens_with_fixed_number(self, expression, number=1):
        return re.sub(r"x\d+", str(number), expression)

    def remove_unmatched_parentheses(self, s):
        stack = []
        indexes_to_remove = set()

        # First pass: Identify unmatched ')' and their positions
        for i, char in enumerate(s):
            if char == "(":
                stack.append(i)  # Store the index of '('
            elif char == ")":
                if stack:
                    stack.pop()  # Match found, remove from stack
                else:
                    indexes_to_remove.add(i)  # Unmatched ')', mark for removal

        # Any remaining '(' in stack are unmatched
        indexes_to_remove.update(stack)

        # Remove unmatched characters
        return "".join(char for i, char in enumerate(s) if i not in indexes_to_remove)
