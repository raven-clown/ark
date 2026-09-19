import React from 'react';
import Header from '../Header';
import Table from '../../components/Table';
import Root from '../../components/Root';
import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { withRouter } from '../../utils/withRouter';
import { uriClientQuotas, uriClientQuota } from '../../utils/endpoints';

const ENTITY_TYPES = ['USER', 'CLIENT_ID', 'IP'];

class ClientQuota extends Root {
  state = {
    data: [],
    loading: true,
    form: { entityType: 'USER', entityName: '', producerByteRate: '', consumerByteRate: '', requestPercentage: '' }
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
      const res = await this.getApi(uriClientQuotas(this.clusterId()));
      this.setState({ data: res.data || [], loading: false });
    } catch (err) {
      this.setState({ loading: false });
    }
  }

  handleFormChange = e => {
    const { name, value } = e.target;
    this.setState({ form: { ...this.state.form, [name]: value } });
  };

  toNumberOrUndefined = value => (value === '' ? undefined : Number(value));

  save = async e => {
    e.preventDefault();
    const { form } = this.state;
    if (!form.entityName) {
      toast.error('Entity name is required');
      return;
    }
    try {
      await this.postApi(uriClientQuotas(this.clusterId()), {
        entityType: form.entityType,
        entityName: form.entityName,
        producerByteRate: this.toNumberOrUndefined(form.producerByteRate),
        consumerByteRate: this.toNumberOrUndefined(form.consumerByteRate),
        requestPercentage: this.toNumberOrUndefined(form.requestPercentage)
      });
      toast.success('Quota saved');
      this.setState({
        form: { entityType: 'USER', entityName: '', producerByteRate: '', consumerByteRate: '', requestPercentage: '' }
      });
      this.load();
    } catch (err) {
      // toasted by the api layer
    }
  };

  remove = async (entityType, entityName) => {
    try {
      await this.removeApi(uriClientQuota(this.clusterId(), entityType, entityName));
      toast.success('Quota removed');
      this.load();
    } catch (err) {
      // toasted by the api layer
    }
  };

  render() {
    const { data, loading, form } = this.state;

    const rows = data.map((entry, i) => ({
      id: `${entry.entityType}-${entry.entityName}-${i}`,
      entityType: entry.entityType,
      entityName: entry.entityName,
      producerByteRate: entry.producerByteRate ?? '-',
      consumerByteRate: entry.consumerByteRate ?? '-',
      requestPercentage: entry.requestPercentage ?? '-',
      raw: entry
    }));

    return (
      <div>
        <Header title="Client Quotas" />

        <Table
          loading={loading}
          columns={[
            { id: 'entityType', accessor: 'entityType', colName: 'Type', sortable: true },
            { id: 'entityName', accessor: 'entityName', colName: 'Entity', sortable: true },
            { id: 'producerByteRate', accessor: 'producerByteRate', colName: 'Producer byte rate' },
            { id: 'consumerByteRate', accessor: 'consumerByteRate', colName: 'Consumer byte rate' },
            { id: 'requestPercentage', accessor: 'requestPercentage', colName: 'Request %' }
          ]}
          actions={['delete']}
          data={rows}
          updateData={updated => this.setState({ data: updated.map(r => r.raw) })}
          onDelete={row => this.remove(row.raw.entityType, row.raw.entityName)}
          noContent="No client quotas set on this cluster."
        />

        <form className="khq-data-filter khq-nav p-3 mt-3" onSubmit={this.save}>
          <div className="row g-2 align-items-end">
            <div className="col-auto">
              <label className="form-label">Entity type</label>
              <select
                className="form-select"
                name="entityType"
                value={form.entityType}
                onChange={this.handleFormChange}
              >
                {ENTITY_TYPES.map(t => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
            </div>
            <div className="col-auto">
              <label className="form-label">Entity name</label>
              <input
                className="form-control"
                name="entityName"
                value={form.entityName}
                onChange={this.handleFormChange}
              />
            </div>
            <div className="col-auto">
              <label className="form-label">Producer byte rate</label>
              <input
                className="form-control"
                name="producerByteRate"
                value={form.producerByteRate}
                onChange={this.handleFormChange}
              />
            </div>
            <div className="col-auto">
              <label className="form-label">Consumer byte rate</label>
              <input
                className="form-control"
                name="consumerByteRate"
                value={form.consumerByteRate}
                onChange={this.handleFormChange}
              />
            </div>
            <div className="col-auto">
              <label className="form-label">Request %</label>
              <input
                className="form-control"
                name="requestPercentage"
                value={form.requestPercentage}
                onChange={this.handleFormChange}
              />
            </div>
            <div className="col-auto">
              <button type="submit" className="btn btn-primary">
                Save quota
              </button>
            </div>
          </div>
          <p className="mt-2 mb-0 text-muted">Leave a field blank to not set it.</p>
        </form>
      </div>
    );
  }
}

export default withRouter(ClientQuota);
