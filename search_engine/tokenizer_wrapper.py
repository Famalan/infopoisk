import os
import subprocess
from typing import List

class TokenizerClient:
    def __init__(self, tokenizer_path: str):
        if not os.path.exists(tokenizer_path):
            raise FileNotFoundError(f"Tokenizer not found: {tokenizer_path}")
        self.process = subprocess.Popen(
            [tokenizer_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            bufsize=1
        )

    def tokenize(self, text: str) -> List[str]:
        if not text:
            return []
        
        sanitized_text = text.replace('\n', ' ')
        
        try:
            self.process.stdin.write(sanitized_text + '\n')
            self.process.stdin.flush()
        except BrokenPipeError:
            return []

        tokens = []
        while True:
            line = self.process.stdout.readline()
            if not line:
                break
            line = line.strip()
            if line == "__END_DOC__":
                break
            if line:
                tokens.append(line)
        return tokens

    def close(self):
        if self.process:
            self.process.stdin.close()
            self.process.stdout.close()
            self.process.terminate()
            self.process.wait()
