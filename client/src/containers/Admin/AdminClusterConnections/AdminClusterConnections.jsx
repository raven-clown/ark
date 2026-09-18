import React from 'react';
import Header from '../../Header';
import Root from '../../../components/Root';
import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { withRouter } from '../../../utils/withRouter';
import { uriAdminClusterConnections, uriAdminClusterConnection } from '../../../utils/endpoints';

class AdminClusterConnections extends Root {
  state = {
    connections: [],
    loading: true,
    showCreate: false,
    form: { name: '', bootstrapServers: '', schemaRegistryUrl: '' },
    expanded: null
  };

  componentDidMount() {
    this.load();
  }

  async load() {
    this.setState({ loading: true });
    try {
      const res = await this.getApi(uriAdminClusterConnections());
      this.setState({ connections: res.data || [], loading: false });
    } catch (err) {
      this.setState({ loading: false });
    }
  }

  handleFormChange = e => {
    const { name, value } = e.target;
    this.setState({ form: { ...this.state.form, [name]: value } });
  };

  create = async e => {
    e.preventDefault();
    const { form } = this.state;
    if (!form.name || !form.bootstrapServers) {
      toast.error('Name and bootstrap servers are required');
      return;
    }
    try {
      await this.postApi(uriAdminClusterConnections(), {
        name: form.name,
        bootstrapServers: form.bootstrapServers,
        schemaRegistryUrl: form.schemaRegistryUrl || null,
        connects: [],
        ksqldbs: []
      });
      toast.success('Cluster connection defined - copy the YAML below into application.yml and restart to activate it');
      this.setState({ showCreate: false, form: { name: '', bootstrapServers: '', schemaRegistryUrl: '' } });
      this.load();
    } catch (err) {
      // toasted by the api layer
    }
  };

  remove = async name => {
    try {
      await this.removeApi(uriAdminClusterConnection(name));
      toast.success('Deleted');
      this.load();
    } catch (err) {
      // toasted by the api layer
    }
  };

  copyYaml = yaml => {
    if (navigator.clipboard) {
      navigator.clipboard.writeText(yaml).then(() => toast.success('YAML copied'));
    }
  };

  render() {
    const { connections, loading, showCreate, form, expanded } = this.state;

    return (
      <div>
        <Header title="Admin - Cluster Connections">
          <button
            className="btn btn-primary ms-2"
            onClick={() => this.setState({ showCreate: !showCreate })}
          >
            {showCreate ? 'Close' : 'Define new cluster'}
          </button>
        </Header>

        <div className="alert alert-warning">
          Defining a cluster here does not make it reachable yet. Copy the generated YAML into{' '}
          <code>application.yml</code> under <code>akhq.connections</code> and restart the server
          to activate it - full hot-reload without a restart is on the roadmap.
        </div>

        {showCreate && (
          <form className="khq-data-filter khq-nav p-3 mb-3" onSubmit={this.create}>
            <div className="row g-2 align-items-end">
              <div className="col-auto">
                <label className="form-label">Cluster name</label>
                <input
                  className="form-control"
                  name="name"
                  placeholder="letters, digits, - and _ only"
                  value={form.name}
                  onChange={this.handleFormChange}
                />
              </div>
              <div className="col-auto">
                <label className="form-label">Bootstrap servers</label>
                <input
                  className="form-control"
                  name="bootstrapServers"
                  placeholder="broker1:9092,broker2:9092"
                  value={form.bootstrapServers}
                  onChange={this.handleFormChange}
                />
              </div>
              <div className="col-auto">
                <label className="form-label">Schema registry URL (optional)</label>
                <input
                  className="form-control"
                  name="schemaRegistryUrl"
                  value={form.schemaRegistryUrl}
                  onChange={this.handleFormChange}
                />
              </div>
              <div className="col-auto">
                <button type="submit" className="btn btn-primary">
                  Save
                </button>
              </div>
            </div>
          </form>
        )}

        {loading ? (
          <p>Loading...</p>
        ) : connections.length === 0 ? (
          <div className="alert alert-warning mb-0">No cluster connections defined yet.</div>
        ) : (
          connections.map(conn => (
            <div className="khq-data-filter khq-nav p-3 mb-3" key={conn.name}>
              <div className="d-flex justify-content-between align-items-center">
                <div>
                  <b>{conn.name}</b> ({conn.bootstrapServers})
                </div>
                <div>
                  <button
                    className="btn btn-secondary btn-sm me-2"
                    onClick={() =>
                      this.setState({ expanded: expanded === conn.name ? null : conn.name })
                    }
                  >
                    {expanded === conn.name ? 'Hide YAML' : 'Show YAML'}
                  </button>
                  <button className="btn btn-danger btn-sm" onClick={() => this.remove(conn.name)}>
                    Delete
                  </button>
                </div>
              </div>
              {expanded === conn.name && (
                <div className="mt-3">
                  <pre className="p-2" style={{ background: '#1e1e1e', color: '#dcdcdc' }}>
                    {conn.yamlSnippet}
                  </pre>
                  <button
                    className="btn btn-secondary btn-sm"
                    onClick={() => this.copyYaml(conn.yamlSnippet)}
                  >
                    Copy YAML
                  </button>
                </div>
              )}
            </div>
          ))
        )}
      </div>
    );
  }
}

export default withRouter(AdminClusterConnections);
