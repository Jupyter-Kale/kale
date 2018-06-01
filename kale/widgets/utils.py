import ipywidgets as ipw

def Space(height=0, width=0):
    "Empty IPyWidget Box of specified size."
    return ipw.Box(
        layout=ipw.Layout(
            width=u'{}px'.format(width),
            height=u'{}px'.format(height)
        )
    )
