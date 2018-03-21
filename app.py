import dash

app = dash.Dash()

# NOTE probably not needed till set up as server
# server = app.server
app.config.suppress_callback_exceptions = True