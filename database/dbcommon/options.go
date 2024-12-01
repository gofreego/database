package dbcommon

type PrepareOption struct {
	name string
}

func (o *PrepareOption) Name() string {
	return o.name
}

func NewPrepareOption(name string) *PrepareOption {
	return &PrepareOption{name: name}
}

func GetPrepareName(options ...any) string {
	for _, option := range options {
		if o, ok := option.(*PrepareOption); ok {
			return o.Name()
		}
	}
	return ""
}
