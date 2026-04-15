package api

type Meta struct {
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
	Total  int `json:"total"`
}

type SuccessResponse struct {
	Data  interface{} `json:"data"`
	Meta  *Meta       `json:"meta,omitempty"`
	Error interface{} `json:"error"`
}

type ErrorResponse struct {
	Data  interface{} `json:"data"`
	Meta  interface{} `json:"meta"`
	Error interface{} `json:"error"`
}

type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
