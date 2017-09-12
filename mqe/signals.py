import logging

from blinker import signal


log = logging.getLogger('mqe.signals')


#: Issued when a new dashboard is created. Keyword arguments:
#:
#: - ``dashboard`` - the newly created :class:`~mqe.dashboards.Dashboard`
new_dashboard = signal('new_dashboard')

#: Issued when a new report is created. Keyword arguments:
#:
#: - ``report`` - the newly created :class:`~mqe.reports.Report`
new_report = signal('new_report')

#: Issued when a dashboard's layout is modified by SSC or TPCreator
#:
#: - ``reason`` - a string describing why the layout was modified: ``'ssc'`` for SSCS
#:   and ``'tpcreator'`` for TPCreator
#: - ``layout_modification_result`` - the :class:`~mqe.layouts.LayoutModificationResult`
#:   describing the modification.
layout_modified = signal('layout_modified')

dao_module_loaded = signal('dao_module_loaded')


def fire_signal(sig, **kwargs):
    #sys.stderr.write('FIRING signal %r\n' % sig)
    from mqe import c
    sender = c
    sig.send(sender, **kwargs)

