solana-keygen new --outfile nonce-1.json

# Check the minimum funding required.
solana rent nonce --url mainnet-beta

# Fund and initialize it; use at least the minimum shown above.
solana create-nonce-account "$HOME/.config/solana/nonces/nonce-1.json" 0.00105665 \
  --url mainnet-beta \
  --keypair /Users/alexnham/Desktop/cs/sol-experience/rust_copy_trader/keypair.json \
  --nonce-authority /Users/alexnham/Desktop/cs/sol-experience/rust_copy_trader/keypair.json

# Print its public key and verify its state.
solana-keygen pubkey "$HOME/.config/solana/nonces/nonce-1.json"
solana nonce-account "$HOME/.config/solana/nonces/nonce-1.json" --url mainnet-beta