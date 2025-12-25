from typing import List

def encode_varbyte(number: int) -> bytes:
    if number < 0:
        raise ValueError("VarByte supports only non-negative integers")
    
    if number == 0:
        return b'\x00'
    
    out = bytearray()
    while number >= 128:
        out.append((number & 0x7F) | 0x80)
        number >>= 7
    out.append(number)
    return bytes(out)

def decode_varbyte_stream(data: bytes, offset: int = 0) -> tuple[int, int]:
    number = 0
    shift = 0
    while True:
        if offset >= len(data):
            raise IndexError("Unexpected end of byte stream")
        
        byte = data[offset]
        offset += 1
        
        number |= (byte & 0x7F) << shift
        
        if not (byte & 0x80):
            break
            
        shift += 7
        
    return number, offset

def encode_delta(numbers: List[int]) -> List[int]:
    if not numbers:
        return []
    
    deltas = [numbers[0]]
    for i in range(1, len(numbers)):
        deltas.append(numbers[i] - numbers[i-1])
    return deltas

def decode_delta(deltas: List[int]) -> List[int]:
    if not deltas:
        return []
        
    numbers = [deltas[0]]
    current = deltas[0]
    for d in deltas[1:]:
        current += d
        numbers.append(current)
    return numbers
