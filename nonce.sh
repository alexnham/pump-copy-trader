solana-keygen new --outfile "$HOME/.config/solana/nonces/nonce-2.json"

# Check the minimum funding required.
solana rent nonce --url mainnet-beta

# Fund and initialize it; use at least the minimum shown above.
solana create-nonce-account "$HOME/.config/solana/nonces/nonce-12.json" 0.00105665 \
  --url mainnet-beta \
  --keypair /Users/alexnham/Desktop/cs/sol-experience/rust_copy_trader/keypair.json \
  --nonce-authority /Users/alexnham/Desktop/cs/sol-experience/rust_copy_trader/keypair.json

# Print its public key and verify its state.
solana-keygen pubkey "$HOME/.config/solana/nonces/nonce-2.json"
solana nonce-account "$HOME/.config/solana/nonces/nonce-2.json" --url mainnet-beta