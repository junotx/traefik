package gateway

func (p *Provider) SetExternalClusterClientFromFile(file string) error {

	client, err := newExternalClusterClientFromFile(file)
	if err != nil {
		return err
	}
	p.client = client
	return nil
}
