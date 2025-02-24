import mmh3

class MurmurHasher:
    """
    MurmurHash3 哈希工具类
    支持32位和128位哈希生成
    """
    
    @staticmethod
    def hash32(data, seed=0):
        """生成32位哈希值"""
        if not isinstance(data, (bytes, str)):
            data = str(data).encode()
        return mmh3.hash(data, seed=seed, signed=False)

    @staticmethod
    def hash128(data, seed=0):
        """生成128位哈希元组"""
        if not isinstance(data, (bytes, str)):
            data = str(data).encode()
        return mmh3.hash128(data, seed=seed, signed=False)

    @staticmethod
    def hash_normalized(data, seed=0, bits=32):
        """生成标准化哈希值（0-1范围）"""
        if bits == 32:
            max_val = 0xFFFFFFFF
            hash_int = MurmurHasher.hash32(data, seed)
        else:
            hash_bytes = MurmurHasher.hash128(data, seed)
            max_val = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
            hash_int = int.from_bytes(hash_bytes.to_bytes(16, 'big'), 'big')
        return hash_int / max_val

if __name__ == "__main__":
    data = "hello world"
    print(f"32-bit hash: {MurmurHasher.hash32(data)}")
    print(f"128-bit hash: {MurmurHasher.hash128(data)}")
    print(f"Normalized 32-bit: {MurmurHasher.hash_normalized(data)}")