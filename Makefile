init: requirements set-perms hooks
	@echo "🎉 All setup!"

requirements:
	@echo "Installing dependencies..."
	pip install -r requirements-dev.txt
	@echo "✅ Dependencies installed."

set-perms:
	@echo "Setting script permissions..."
	chmod -R +x ./scripts/
	@echo "✅ Permissions set."

hooks:
	@echo "Installing Git hooks..."
	pre-commit install
	pre-commit install --hook-type commit-msg
	@echo "✅ Git hooks installed."
