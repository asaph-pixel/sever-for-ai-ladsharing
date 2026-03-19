use aes::Aes256;
use cbc::Encryptor;
use cbc::cipher::{BlockEncryptMut, KeyIvInit};
use block_padding::Pkcs7;

type Aes256CbcEnc = Encryptor<Aes256>;

pub fn encrypt(data: &[u8], key: &[u8], iv: &[u8]) -> Vec<u8> {
    use cbc::cipher::block_padding::Pkcs7;
    let cipher = Aes256CbcEnc::new_from_slices(key, iv).unwrap();
    let mut buf = vec![0u8; data.len() + 16];
    buf[..data.len()].copy_from_slice(data);
    let len = cipher.encrypt_padded_mut::<Pkcs7>(&mut buf, data.len()).unwrap().len();
    buf[..len].to_vec()
}
pub fn generate_key_iv() -> (Vec<u8>, Vec<u8>) {
    let key: Vec<u8> = (0..32).map(|_| rand::random::<u8>()).collect();
    let iv: Vec<u8> = (0..16).map(|_| rand::random::<u8>()).collect();
    (key, iv)
}