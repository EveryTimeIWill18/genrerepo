

class OrderedList(object):
    """
    OrderedList
    ===========
    List class that maintains an ordering
    of the data that has been inserted into it.
    Data positions can be changed if need be.
    """

    def __init__(self, *args):
        self.ord_list = None
        self.counter = 0
        self.initial_args = []
        for a in args:
            self.initial_args.append((self.counter, a))
            self.counter += 1
        if len(self.initial_args) > 0:
            self.ord_list = self.initial_args

    def __setitem__(self, value):
        self.ord_list[self.counter] = (self.counter, value)

    def __getitem__(self, idx):
        assert type(idx) == int
        try:
            if idx < self.counter:
               return self.ord_list[idx]
            else:
                raise IndexError("IndexError: Index is out of range.")
        except IndexError as e:
            return e
        finally:
            pass

    def __len__(self):
        return len(self.ord_list)

    def contents(self):
        if self.ord_list.__len__() > 0:
            for item in self.ord_list:
                print("item: {}".format(item))

    def __delitem__(self, item):
        if type(item) == int:
            item_len = len(str(item))
            sub_list = [i for i in self.ord_list
                            if type(i[1]) == int
                            and len(str(i[1])) == item_len
                        ]
            if len(sub_list) == 1:
                deleted = sub_list[0]
                del self.ord_list[deleted[0]]
                print("Deleting: {}".format(deleted))
                return None
            else:
                for i in sub_list:
                    if i[1] == item:
                        return i[1]

        if type(item) == str:
            print "Item is of type string"
            item_len = len(str(item))
            sub_list = [i for i in self.ord_list
                            if type(i[1]) == str
                            and len(str(i[1])) == item_len
                        ]
            if len(sub_list) == 1:
                deleted = sub_list[0]
                del self.ord_list[deleted[0]]
                print("Deleting: {}".format(deleted))
                return None
            else:
                for i in sub_list:
                    if i[1] == item:
                        return i[1]
        else:
            print("Type is unsupported")




ord_list = OrderedList('hello', 'there', 'this', 100, 20)

ord_list.contents()

print ord_list.__delitem__('this')

ord_list.contents()

class Pipeline:
    """
    Pipeline
    ========
    Dynamically adds tasks to be run.
    Rather than using functions as decorators, we
    can also use instance methods.

    :parameter
        tasks: stores tasks that will be run in
            order of when they should be executed.
    """

    def __init__(self):
        self.tasks = []
        self.task_dict = dict()

    def task(self):
        def inner(f):
            """
            Adds a task to the task list.
            :return: None
            """
            self.tasks.append(f)
            task_counter = 1
            print "Function Name: {}".format(f.__name__)
            print "Added {} tasks to the list".format(task_counter)
            del task_counter
            return f
        return inner


