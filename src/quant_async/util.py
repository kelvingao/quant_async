

# ---------------------------------------------
def create_ib_tuple(instrument):
    """ create ib contract tuple """
    from quant_async import futures

    if isinstance(instrument, str):
        instrument = instrument.upper()

        if "FUT." not in instrument:
            # symbol stock
            instrument = (instrument, "STK", "SMART", "USD", "", 0.0, "")

        else:
            # future contract
            try:
                symdata = instrument.split(".")

                # is this a CME future?
                if symdata[1] not in futures.futures_contracts.keys():
                    raise ValueError(
                        "Un-supported symbol. Please use full contract tuple.")

                # auto get contract details
                spec = futures.get_ib_futures(symdata[1])
                if not isinstance(spec, dict):
                    raise ValueError("Un-parsable contract tuple")

                # expiry specified?
                if len(symdata) == 3 and symdata[2] != '':
                    expiry = symdata[2]
                else:
                    # default to most active
                    expiry = futures.get_active_contract(symdata[1])

                instrument = (spec['symbol'].upper(), "FUT",
                              spec['exchange'].upper(), spec['currency'].upper(),
                              int(expiry), 0.0, "")

            except Exception:
                raise ValueError("Un-parsable contract tuple")

    # tuples without strike/right
    elif len(instrument) <= 7:
        instrument_list = list(instrument)
        if len(instrument_list) < 3:
            instrument_list.append("SMART")
        if len(instrument_list) < 4:
            instrument_list.append("USD")
        if len(instrument_list) < 5:
            instrument_list.append("")
        if len(instrument_list) < 6:
            instrument_list.append(0.0)
        if len(instrument_list) < 7:
            instrument_list.append("")

        try:
            instrument_list[4] = int(instrument_list[4])
        except Exception:
            pass

        instrument_list[5] = 0. if isinstance(instrument_list[5], str) \
            else float(instrument_list[5])

        instrument = tuple(instrument_list)

    return instrument