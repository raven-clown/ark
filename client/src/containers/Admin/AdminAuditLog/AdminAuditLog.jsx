import React from 'react';
import Header from '../../Header';
import Table from '../../../components/Table';
import Root from '../../../components/Root';
import { withRouter } from '../../../utils/withRouter';
import { uriAdminAuditLog, uriAdminTeams } from '../../../utils/endpoints';

const ACTION_TYPES = [
  'CONSUMER_GROUP_UPDATE_OFFSETS',
  'CONSUMER_GROUP_DELETE_OFFSETS',
  'CONSUMER_GROUP_DELETE',
  'TOPIC_CREATE',
  'TOPIC_CONFIG_CHANGE',
  'TOPIC_INCREASE_PARTITION',
  'TOPIC_DELETE',
  'RECORD_PRODUCE',
  'RECORD_DELETE',
  'RECORD_EMPTY',
  'SCHEMA_CREATE',
  'SCHEMA_UPDATE',
  'SCHEMA_COMPATIBILITY_UPDATE',
  'SCHEMA_DELETE',
  'CONNECT_CREATE',
  'CONNECT_UPDATE',
  'CONNECT_DELETE',
  'CONNECT_RESTART',
  'CONNECT_TASK_RESTART',
  'CONNECT_PAUSE',
  'CONNECT_RESUME'
];

function toEpochMillis(datetimeLocalValue) {
  if (!datetimeLocalValue) return undefined;
  const parsed = new Date(datetimeLocalValue);
  return Number.isNaN(parsed.getTime()) ? undefined : parsed.getTime();
}

class AdminAuditLog extends Root {
  state = {
    allTeams: [],
    filters: { employeeCode: '', team: '', actionType: '', from: '', to: '' },
    entries: [],
    loading: false,
    searched: false
  };

  componentDidMount() {
    this.getApi(uriAdminTeams()).then(res => this.setState({ allTeams: res.data || [] }));
  }

  handleFilterChange = e => {
    const { name, value } = e.target;
    this.setState({ filters: { ...this.state.filters, [name]: value } });
  };

  search = async e => {
    if (e) e.preventDefault();
    const { filters } = this.state;
    this.setState({ loading: true });
    try {
      const res = await this.getApi(
        uriAdminAuditLog({
          employeeCode: filters.employeeCode,
          team: filters.team,
          actionType: filters.actionType,
          fromEpochMilli: toEpochMillis(filters.from),
          toEpochMilli: toEpochMillis(filters.to)
        })
      );
      this.setState({ entries: res.data || [], loading: false, searched: true });
    } catch (err) {
      this.setState({ loading: false, searched: true });
    }
  };

  render() {
    const { allTeams, filters, entries, loading, searched } = this.state;

    const rows = entries.map((entry, i) => ({
      id: `${entry.partition}-${entry.offset}-${i}`,
      timestamp: entry.timestamp ? new Date(entry.timestamp).toLocaleString() : '',
      userName: entry.userName,
      type: entry.type,
      actionType: entry.actionType,
      details: entry.details ? JSON.stringify(entry.details) : ''
    }));

    return (
      <div>
        <Header title="Admin - Audit Log">
          <button className="btn btn-secondary ms-2" onClick={() => window.print()}>
            Print
          </button>
        </Header>

        <form className="khq-data-filter khq-nav p-3 mb-3" onSubmit={this.search}>
          <div className="row g-2 align-items-end">
            <div className="col-auto">
              <label className="form-label">Employee code</label>
              <input
                className="form-control"
                name="employeeCode"
                value={filters.employeeCode}
                onChange={this.handleFilterChange}
              />
            </div>
            <div className="col-auto">
              <label className="form-label">Team</label>
              <select
                className="form-select"
                name="team"
                value={filters.team}
                onChange={this.handleFilterChange}
              >
                <option value="">All teams</option>
                {allTeams.map(t => (
                  <option key={t.name} value={t.name}>
                    {t.name}
                  </option>
                ))}
              </select>
            </div>
            <div className="col-auto">
              <label className="form-label">Action type</label>
              <select
                className="form-select"
                name="actionType"
                value={filters.actionType}
                onChange={this.handleFilterChange}
              >
                <option value="">All actions</option>
                {ACTION_TYPES.map(a => (
                  <option key={a} value={a}>
                    {a}
                  </option>
                ))}
              </select>
            </div>
            <div className="col-auto">
              <label className="form-label">From</label>
              <input
                type="datetime-local"
                className="form-control"
                name="from"
                value={filters.from}
                onChange={this.handleFilterChange}
              />
            </div>
            <div className="col-auto">
              <label className="form-label">To</label>
              <input
                type="datetime-local"
                className="form-control"
                name="to"
                value={filters.to}
                onChange={this.handleFilterChange}
              />
            </div>
            <div className="col-auto">
              <button type="submit" className="btn btn-primary">
                Search
              </button>
            </div>
          </div>
          <p className="mt-2 mb-0 text-muted">
            Default range is the last 24 hours when no &quot;From&quot; is set. Results are capped
            at 500 rows and 20,000 scanned records per search.
          </p>
        </form>

        {searched && (
          <Table
            loading={loading}
            columns={[
              { id: 'timestamp', accessor: 'timestamp', colName: 'When', sortable: true },
              { id: 'userName', accessor: 'userName', colName: 'Employee', sortable: true },
              { id: 'type', accessor: 'type', colName: 'Type' },
              { id: 'actionType', accessor: 'actionType', colName: 'Action' },
              { id: 'details', accessor: 'details', colName: 'Details' }
            ]}
            actions={[]}
            data={rows}
            updateData={() => {}}
            noContent="No audit events match this search."
          />
        )}
      </div>
    );
  }
}

export default withRouter(AdminAuditLog);
