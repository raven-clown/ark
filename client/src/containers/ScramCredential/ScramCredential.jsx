import React from 'react';
import Header from '../Header';
import Table from '../../components/Table';
import Root from '../../components/Root';
import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { withRouter } from '../../utils/withRouter';
import { uriScramCredentials, uriScramCredential } from '../../utils/endpoints';

const MECHANISMS = ['SCRAM_SHA_256', 'SCRAM_SHA_512'];

class ScramCredential extends Root {
  state = {
    data: [],
    loading: true,
    form: { username: '', mechanism: 'SCRAM_SHA_256', password: '', iterations: 4096 }
  };

  componentDidMount() {
    this.load();
  }

  clusterId() {
    return this.props.params.clusterId;
  }

  async load() {
    this.setState({ loading: true });
    try {
      const res = await this.getApi(uriScramCredentials(this.clusterId()));
      this.setState({ data: res.data || [], loading: false });
    } catch (err) {
      this.setState({ loading: false });
    }
  }

  handleFormChange = e => {
    const { name, value } = e.target;
    this.setState({ form: { ...this.state.form, [name]: value } });
  };

  save = async e => {
    e.preventDefault();
    const { form } = this.state;
    if (!form.username || !form.password || form.password.length < 8) {
      toast.error('Username and a password of at least 8 characters are required');
      return;
    }
    try {
      await this.postApi(uriScramCredentials(this.clusterId()), {
        username: form.username,
        mechanism: form.mechanism,
        password: form.password,
        iterations: Number(form.iterations) || 4096
      });
      toast.success('Credential saved');
      this.setState({ form: { username: '', mechanism: 'SCRAM_SHA_256', password: '', iterations: 4096 } });
      this.load();
    } catch (err) {
      // toasted by the api layer
    }
  };

  remove = async (username, mechanism) => {
    try {
      await this.removeApi(uriScramCredential(this.clusterId(), username, mechanism));
      toast.success('Credential removed');
      this.load();
    } catch (err) {
      // toasted by the api layer
    }
  };

  render() {
    const { data, loading, form } = this.state;

    const rows = data.map((entry, i) => ({
      id: `${entry.username}-${entry.mechanism}-${i}`,
      username: entry.username,
      mechanism: entry.mechanism,
      iterations: entry.iterations,
      raw: entry
    }));

    return (
      <div>
        <Header title="SASL/SCRAM Credentials" />

        <div className="alert alert-warning">
          Kafka never returns the password back out. Saving here always sets a brand new one, it
          never shows or confirms what is currently set.
        </div>

        <Table
          loading={loading}
          columns={[
            { id: 'username', accessor: 'username', colName: 'Username', sortable: true },
            { id: 'mechanism', accessor: 'mechanism', colName: 'Mechanism' },
            { id: 'iterations', accessor: 'iterations', colName: 'Iterations' }
          ]}
          actions={['delete']}
          data={rows}
          updateData={updated => this.setState({ data: updated.map(r => r.raw) })}
          onDelete={row => this.remove(row.raw.username, row.raw.mechanism)}
          noContent="No SASL/SCRAM credentials on this cluster."
        />

        <form className="khq-data-filter khq-nav p-3 mt-3" onSubmit={this.save}>
          <div className="row g-2 align-items-end">
            <div className="col-auto">
              <label className="form-label">Username</label>
              <input
                className="form-control"
                name="username"
                value={form.username}
                onChange={this.handleFormChange}
              />
            </div>
            <div className="col-auto">
              <label className="form-label">Mechanism</label>
              <select
                className="form-select"
                name="mechanism"
                value={form.mechanism}
                onChange={this.handleFormChange}
              >
                {MECHANISMS.map(m => (
                  <option key={m} value={m}>
                    {m}
                  </option>
                ))}
              </select>
            </div>
            <div className="col-auto">
              <label className="form-label">Password</label>
              <input
                type="password"
                className="form-control"
                name="password"
                value={form.password}
                onChange={this.handleFormChange}
              />
            </div>
            <div className="col-auto">
              <label className="form-label">Iterations</label>
              <input
                className="form-control"
                name="iterations"
                value={form.iterations}
                onChange={this.handleFormChange}
              />
            </div>
            <div className="col-auto">
              <button type="submit" className="btn btn-primary">
                Save credential
              </button>
            </div>
          </div>
        </form>
      </div>
    );
  }
}

export default withRouter(ScramCredential);
