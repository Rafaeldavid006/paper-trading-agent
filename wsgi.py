from webapp import create_app
from paper_readonly import build_readonly_connection

# Existing server-held credentials are consumed only by a GET-only Paper client.
# No execution adapter is constructed; this startup probe cannot place/cancel orders.
app = create_app(paper_adapter=build_readonly_connection())
