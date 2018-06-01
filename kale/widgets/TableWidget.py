import ipywidgets as ipw


class RowWidget(ipw.HBox):
    "Row associated with one table."

    def __init__(self, row, table, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table = table
        self.children = self.parse_row(row)
        self.layout = table.row_layout

    def parse_row(self, row):
        "Create usable widgets from row contents"
        cell_boxes = []
        for i, cell in enumerate(row):
            if type(cell) is str:
                cell_widget = ipw.HTML(cell)
            else:
                cell_widget = cell

            cell_boxes.append(
                ipw.HBox(
                    [cell_widget],
                    layout=self.table.cell_layouts[i],
                    )
                )

        return cell_boxes

    def get_index(self):
        "Get index of this row in the table"
        return self.table.children.index(self)


class TableWidget(ipw.VBox):
    def __init__(self, contents, align='left', width=600, col_widths=None):
        "Contents should be a rectangular list of lists or numpy array"
        super().__init__()

        if type(width) is int:
            self.width_u = u'{}px'.format(width)
            self.width_int = width
        else:
            try:
                if '%' in width:
                    raise NotImplementedError("Specify absolute width, not %.")
            except TypeError:
                pass

            self.width_u = u'{}'.format(width)
            self.width_int = int(width.strip('px'))

        self.num_cols = max([len(row) for row in contents])

        # By default, make columns equal in length
        if col_widths is None:
            try:
                self.col_width_int = self.width_int // self.num_cols
            except ZeroDivisionError:
                self.col_width_int = 0
            self.col_widths_int = [self.col_width_int] * self.num_cols
            self.col_widths_u = [u'{}px'.format(width_int) for width_int in self.col_widths_int]

        # If `col_widths` is specified, override `width`.
        else:
            try:
                if len(col_widths) == self.num_cols:
                    self.col_widths_int = col_widths
                    self.col_widths_u = [u'{}px'.format(width_int) for width_int in col_widths]
                    self.width_int = sum(col_widths)
                    self.width_u = u'{}px'.format(self.width_int)
                else:
                    raise ValueError("Incorrect number of column widths provided.")
            # If len(col_widths) doesn't make sense
            except TypeError:
                raise ValueError("col_widths should be an iterable.")

        # Row alignment
        if align == 'left':
            flex_align = 'flex-start'
        elif align == 'right':
            flex_align = 'flex-end'
        else:
            flex_align = align

        self.layout = ipw.Layout(
            width=self.width_u
            )

        self.row_layout = ipw.Layout(
            width=self.width_u,
            justify_content='space-between'
            )
        self.cell_layouts = [
            ipw.Layout(
                width=col_width,
                justify_content=flex_align
                )
            for col_width in self.col_widths_u
            ]

        self.from_lists(contents)

    def from_lists(self, contents):
        "Create table from lists"
        for row in contents:
            self.add_row(row)

    def add_row(self, row):
        "Append row to end of table"
        row_box = RowWidget(row, table=self, layout=self.row_layout)
        self.children += (row_box,)

    def insert_row(self, index, row):
        "Insert `row` at before row `index`"
        row_box = RowWidget(row, table=self, layout=self.row_layout)
        new_children = list(self.children)
        new_children.insert(index, row_box)
        self.children = new_children

    def pop_row(self, index):
        "Pop row from table, as in `list.pop`."
        row_list = list(self.children)
        old = row_list.pop(index)
        self.children = row_list
        return old
