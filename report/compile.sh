#!/bin/bash
# Compile report.tex to PDF (requires texlive).
set -eu
cd "$(dirname "$0")"

if ! command -v pdflatex >/dev/null 2>&1; then
  echo "Installing minimal TeX Live (one-time, ~200MB)..."
  sudo apt-get update -qq
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq texlive-latex-base texlive-latex-recommended texlive-lang-european
fi

pdflatex -interaction=nonstopmode report.tex
pdflatex -interaction=nonstopmode report.tex
echo "PDF: $(pwd)/report.pdf"
