#!/bin/bash
# Install Amp using ampup

set -e

echo "Installing ampup..."
curl --proto '=https' --tlsv1.2 -sSf https://ampup.sh/install | sh

echo ""
echo "Installation complete!"
echo ""
echo "Please restart your terminal or run:"
echo "  source ~/.zshenv"
echo ""
echo "Then install Amp with:"
echo "  ampup install"
