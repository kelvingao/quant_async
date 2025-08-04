class Instrument(str):
    """A string subclass that provides easy access to misc
    symbol-related methods and information.
    """

    parent = None
    tick_window = None
    bar_window = None

    # ---------------------------------------
    def _set_parent(self, parent):
        """ sets the parent object to communicate with """
        self.parent = parent

    # ---------------------------------------
    def _set_windows(self, ticks, bars):
        """ be aware of default windows """
        self.tick_window = ticks
        self.bar_window = bars

    # ---------------------------------------
    def get_quote(self):
        """ Get last quote for this instrument

        :Retruns:
            quote : dict
                The quote for this instruments
        """
        if self in self.parent.quotes.keys():
            return self.parent.quotes[self]
        return None
    # ---------------------------------------
    def get_orderbook(self):
        """Get orderbook for the instrument

        :Retruns:
            orderbook : dict
                orderbook dict for the instrument
        """
        if self in self.parent.books.keys():
            return self.parent.books[self]

        return {
            "bid": [0], "bidsize": [0],
            "ask": [0], "asksize": [0]
        }

    # ---------------------------------------
    def get_position(self, attr=None):
        """Get the positions data for the instrument

        :Optional:
            attr : string
                Position attribute to get
                (optional attributes: symbol, position, avgCost, account)

        :Retruns:
            positions : dict (positions) / float/str (attribute)
                positions data for the instrument
        """
        pos = self.parent.get_position(self)
        try:
            if attr is not None:
                attr = attr.replace("quantity", "position")
            return pos[attr]
        except Exception:
            return pos

    # ---------------------------------------
    @property
    def position(self):
        """(Property) Shortcut to self.get_positions()"""
        return self.get_position()