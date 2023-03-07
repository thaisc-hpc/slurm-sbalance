class FieldConfig:
    def __init__(self, field:str, header:str, topic:str=None, size:int=10, str_disp:str="{:<%d}", field_disp:str="{:<%d}"):
        self.field =field
        self.header =header
        self.topic =topic
        self.size =size
        self._str_disp =str_disp
        self._field_disp =field_disp
        self._map =map

    @property
    def str_disp(self) -> str:
        return self._str_disp % self.size
    
    @property
    def field_disp(self) -> str:
        return self._field_disp % self.size

FIELD_CONFIGS={
    "account": FieldConfig(field="account", header="Account", size=10, str_disp="{:<%d}", field_disp="{:<%d}"),
    "description": FieldConfig(field="description", header="Descr", size=7, str_disp="{:<%d}", field_disp="{:<%d}"),

    # Service Hour
    "allocation": FieldConfig(field="sh_alloc", header="Allocation(SHr)", size=16, topic="Service Hour", str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    "remaining": FieldConfig(field="sh_remaining", header="Remaining(SHr)", size=16, topic="Service Hour", str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    "used": FieldConfig(field="sh_used", header="Used(SHr)", size=16, topic="Service Hour", str_disp="{:>%d}", field_disp="{:>%d.2f}"),

    # System SU
    "allocation_system": FieldConfig(field="su_alloc", header="Allocation(SU)", topic="Service Unit", size=14, str_disp="{:>%d}", field_disp="{:>%d.0f}"),
    "remaining_system": FieldConfig(field="su_remaining", header="Remaining(SU)", topic="Service Unit", size=12, str_disp="{:>%d}", field_disp="{:>%d.0f}"),
    "used_system": FieldConfig(field="su_used", header="Used(SU)", topic="Service Unit", size=12, str_disp="{:>%d}", field_disp="{:>%d.0f}"),

    "used_percent": FieldConfig(field="percent_used", header="Used(%)", size=12, str_disp="{:>%d}", field_disp="{:>%d.2%%}"),
    "remaining_percent": FieldConfig(field="percent_remaining", header="Remaining(%)", size=12, str_disp="{:>%d}", field_disp="{:>%d.2%%}"),
    
    "allocation_compute": FieldConfig(field="su_alloc_compute",  header="Alloc(Node-Hr)", topic="Compute (Estimated)", size=16, str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    "remaining_compute": FieldConfig(field="su_remaining_compute", header="Remain(Node-Hr)", topic="Compute (Estimated)", size=16, str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    "used_compute": FieldConfig(field="su_used_compute", header="Used(Node-Hr)", topic="Compute (Estimated)", size=16, str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    
    "allocation_gpu": FieldConfig(field="su_alloc_gpu", header="Alloc(Node-Hr)", topic="GPU (Estimated)", size=16, str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    "remaining_gpu": FieldConfig(field="su_remaining_gpu", header="Remain(Node-Hr)", topic="GPU (Estimated)", size=16, str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    "used_gpu": FieldConfig(field="su_used_gpu", header="Used(Node-Hr)", topic="GPU (Estimated)", size=16, str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    
    "allocation_memory": FieldConfig(field="su_alloc_memory", header="Allocation(Hr)", topic="Memory (Estimated)", size=16, str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    "remaining_memory": FieldConfig(field="su_remaining_memory", header="Remaining(Hr)", topic="Memory (Estimated)", size=16, str_disp="{:>%d}", field_disp="{:>%d.2f}"),
    "used_memory": FieldConfig(field="su_used_memory", header="Used(Hr)", size=16, topic="Memory (Estimated)", str_disp="{:>%d}", field_disp="{:>%d.2f}"),
}

FIELD_PER_USER_CONFIGS={
    "account": FieldConfig(field="account", header="Account", size=10, str_disp="{:<%d}", field_disp="{:<%d}"),
    "user": FieldConfig(field="user", header="User", size=12, str_disp="{:<%d}", field_disp="{:<%d}"),
    "used_percent": FieldConfig(field="percent_used", header="Used(%)", size=12, str_disp="{:>%d}", field_disp="{:>%d.2%%}"),
    "used": FieldConfig(field="sh_used", header="Used(SHr)", size=16, topic="Service Hour", str_disp="{:>%d}", field_disp="{:>%d.2f}"),
}