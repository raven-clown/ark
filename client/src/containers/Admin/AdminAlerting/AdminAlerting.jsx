import React from 'react';
import Header from '../../Header';
import Root from '../../../components/Root';
import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { withRouter } from '../../../utils/withRouter';
import { uriAdminTeams, uriTeamWebhooks, uriTeamWebhook } from '../../../utils/endpoints';

const WEBHOOK_TYPES = ['SLACK', 'LINE', 'GENERIC'];

class AdminAlerting extends Root {
  state = {
    teams: [],
    selectedTeam: '',
    webhooks: [],
    loading: true,
    form: { webhookType: 'SLACK', webhookUrl: '', lineToken: '' }
  };

  componentDidMount() {
    this.loadTeams();
  }

  async loadTeams() {
    this.setState({ loading: true });
    try {
      const res = await this.getApi(uriAdminTeams());
      const teams = res.data || [];
      const selectedTeam = teams.length > 0 ? teams[0].name : '';
      this.setState({ teams, selectedTeam, loading: false }, () => {
        if (selectedTeam) this.loadWebhooks();
      });
    } catch (err) {
      this.setState({ loading: false });
    }
  }

  async loadWebhooks() {
    const { selectedTeam } = this.state;
    if (!selectedTeam) return;
    try {
      const res = await this.getApi(uriTeamWebhooks(selectedTeam));
      this.setState({ webhooks: res.data || [] });
    } catch (err) {
      // toasted by the api layer
    }
  }

  selectTeam = e => {
    this.setState({ selectedTeam: e.target.value }, () => this.loadWebhooks());
  };

  handleFormChange = e => {
    const { name, value } = e.target;
    this.setState({ form: { ...this.state.form, [name]: value } });
  };

  addWebhook = async e => {
    e.preventDefault();
    const { form, selectedTeam } = this.state;
    if (!form.webhookUrl) {
      toast.error('Webhook URL is required');
      return;
    }
    try {
      await this.postApi(uriTeamWebhooks(selectedTeam), form);
      toast.success('Webhook added');
      this.setState({ form: { webhookType: 'SLACK', webhookUrl: '', lineToken: '' } });
      this.loadWebhooks();
    } catch (err) {
      // toasted by the api layer
    }
  };

  removeWebhook = async id => {
    const { selectedTeam } = this.state;
    try {
      await this.removeApi(uriTeamWebhook(selectedTeam, id));
      toast.success('Webhook removed');
      this.loadWebhooks();
    } catch (err) {
      // toasted by the api layer
    }
  };

  render() {
    const { teams, selectedTeam, webhooks, loading, form } = this.state;

    if (loading) {
      return (
        <div>
          <Header title="Admin - Alerting" />
        </div>
      );
    }

    return (
      <div>
        <Header title="Admin - Alerting" />

        <div className="alert alert-warning">
          Alerting must also be turned on in the server config (<code>akhq.alerting.enabled</code>)
          before any webhook below actually fires. A team gets notified only for clusters it
          already has a permission grant on.
        </div>

        <div className="khq-data-filter khq-nav p-3 mb-3">
          <label className="form-label">Team</label>
          <select className="form-select" value={selectedTeam} onChange={this.selectTeam}>
            {teams.map(t => (
              <option key={t.name} value={t.name}>
                {t.name}
              </option>
            ))}
          </select>
        </div>

        <h3>Webhooks</h3>
        <div className="khq-data-filter khq-nav p-3 mb-3">
          <table className="table">
            <thead>
              <tr>
                <th>Type</th>
                <th>URL</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {webhooks.map(w => (
                <tr key={w.id}>
                  <td>{w.webhookType}</td>
                  <td>{w.webhookUrl}</td>
                  <td>
                    <button className="btn btn-danger btn-sm" onClick={() => this.removeWebhook(w.id)}>
                      Remove
                    </button>
                  </td>
                </tr>
              ))}
              {webhooks.length === 0 && (
                <tr>
                  <td colSpan={3}>No webhook configured for this team yet.</td>
                </tr>
              )}
            </tbody>
          </table>

          <form className="row g-2 align-items-end" onSubmit={this.addWebhook}>
            <div className="col-auto">
              <label className="form-label">Type</label>
              <select
                className="form-select"
                name="webhookType"
                value={form.webhookType}
                onChange={this.handleFormChange}
              >
                {WEBHOOK_TYPES.map(t => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
            </div>
            <div className="col-auto">
              <label className="form-label">
                {form.webhookType === 'LINE' ? 'Label' : 'Webhook URL'}
              </label>
              <input
                className="form-control"
                name="webhookUrl"
                value={form.webhookUrl}
                onChange={this.handleFormChange}
              />
            </div>
            {form.webhookType === 'LINE' && (
              <div className="col-auto">
                <label className="form-label">LINE Notify token</label>
                <input
                  className="form-control"
                  name="lineToken"
                  value={form.lineToken}
                  onChange={this.handleFormChange}
                />
              </div>
            )}
            <div className="col-auto">
              <button type="submit" className="btn btn-primary">
                Add webhook
              </button>
            </div>
          </form>
        </div>
      </div>
    );
  }
}

export default withRouter(AdminAlerting);
